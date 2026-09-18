package locking

import (
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

// FileLock is the driven adapter satisfying the coordination port with a lease kept in a
// file on the shared filesystem.
//
// It asks two things of that filesystem, and gets both from a real one: that O_CREATE|O_EXCL
// is atomic, which is what makes a claim on a free lock a claim rather than an open, and that
// a rename replaces a file atomically, which is what stops a reader seeing a half-written
// holder. afero's in-memory filesystem provides neither — its OpenFile checks for the file and
// creates it under separate locks — so a FileLock over MemMapFs can hand the same lease to two
// instances. That is not a deployment: the server takes this lock only when it is not running
// in memory.
var _ driven.SingleIbsenWriterLock = &FileLock{}

type FileLock struct {
	afero        *afero.Afero
	lockFile     string
	reclaimLease time.Duration
	leaseTime    time.Duration
	uniqueId     string
	onLost       LeaseLost
	// mu serializes lease renewals with ReleaseLock; stop ends the renewals once released
	mu       *sync.Mutex
	stop     chan struct{}
	stopOnce *sync.Once
}

// LeaseLost is called when the lease can no longer be proven to be held: a renewal that could
// not be written, one that found the lock file naming somebody else, or one that arrived after
// its own lease had already aged out. Whatever it does, this process must stop writing
// immediately, because another instance may already be writing.
//
// A clean shutdown is the wrong answer: it flushes and writes, which is exactly what must not
// happen now. The composition root decides, the way it decides what a refused lock means; it
// must not call back into the lock.
type LeaseLost func(reason error)

func NewFileLock(afero *afero.Afero, lockFile string, leaseTime time.Duration, waitFor time.Duration, onLost LeaseLost) FileLock {
	if onLost == nil {
		// a caller that has not thought about it must not get a no-op: losing the lease and
		// carrying on writing is the one outcome this lock exists to prevent
		onLost = func(reason error) {
			log.Fatal().Err(reason).Msgf("lost the single writer lock %s", lockFile)
		}
	}
	return FileLock{
		afero:        afero,
		lockFile:     lockFile,
		reclaimLease: waitFor,
		leaseTime:    leaseTime,
		uniqueId:     uuid.New().String(),
		onLost:       onLost,
		mu:           &sync.Mutex{},
		stop:         make(chan struct{}),
		stopOnce:     &sync.Once{},
	}
}

// AcquireLock claims the single-writer lease, and reports whether this instance got it.
//
// There are two ways to claim, and only one of them is atomic. A lock file that does not exist
// yet is created with O_CREATE|O_EXCL, which is the one primitive a filesystem offers here:
// of two instances starting at the same moment exactly one creates the file and the other is
// told it already exists. A lease whose holder has gone quiet has to be taken over instead,
// and there is no compare-and-swap to do that with, so the claim is confirmed rather than
// assumed. See takeOverExpired.
func (fl FileLock) AcquireLock() bool {
	exists, err := fl.afero.Exists(fl.lockFile)
	if err != nil {
		log.Err(err).Msgf("failed while checking if file %s exists", fl.lockFile)
		return false
	}
	if !exists {
		claimed, err := fl.createExclusively()
		if err != nil {
			log.Err(err).Msgf("failed while claiming lock file %s", fl.lockFile)
			return false
		}
		if !claimed {
			// somebody created it between the check and the create, so they hold a fresh lease
			log.Info().Msgf("lock file %s was claimed by another instance", fl.lockFile)
			return false
		}
		go fl.reclaimer()
		return true
	}

	owner, err := fl.readOwner()
	if err != nil {
		log.Err(err).Msgf("failed while reading lock file %s", fl.lockFile)
		return false
	}
	if owner == fl.uniqueId {
		// this instance already holds it: AcquireLock is a startup call, not a renewal
		log.Warn().Msgf("lock file %s is already held by this instance", fl.lockFile)
		return false
	}
	modified, err := fl.getFileModificationTime()
	if err != nil {
		log.Err(err).Msgf("unable to get modification time for file %s", fl.lockFile)
		return false
	}
	if time.Since(modified) < fl.leaseTime {
		// somebody else holds a live lease
		return false
	}
	if !fl.takeOverExpired() {
		return false
	}
	go fl.reclaimer()
	return true
}

// createExclusively claims a lock file that does not exist yet, and reports false when
// somebody else created it first. O_CREATE|O_EXCL makes that a single atomic step, so two
// instances starting together cannot both come away believing they hold the lease — which
// they could when this used O_CREATE alone and both simply wrote their id.
func (fl FileLock) createExclusively() (bool, error) {
	file, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0660)
	if err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	_, writeErr := file.Write([]byte(fl.uniqueId))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		// an empty lock file would block every other instance until it aged out, and it is
		// ours to clean up because we are the one that created it
		if removeErr := fl.afero.Remove(fl.lockFile); removeErr != nil {
			log.Err(removeErr).Msgf("unable to remove the lock file %s this instance failed to claim", fl.lockFile)
		}
		return false, errore.WrapError(closeErr, writeErr)
	}
	return true, nil
}

// takeOverExpired claims a lease whose holder has gone quiet, and reports whether this
// instance got it.
//
// A filesystem has no compare-and-swap, so two instances can both find the same lease expired
// and both write their id over it. The claim is therefore confirmed rather than assumed: after
// a pause long enough for a competing claim to land, the file is read back, and only the
// instance whose id is in it carries on. Both backing off is a safe outcome and both proceeding
// is not, so anything unrecognised in the file is read as "not mine".
//
// This is not provably atomic, and nothing on a plain filesystem can make it so. What makes it
// safe in the end is renewal: it stops the moment it cannot prove the lease is still its own,
// so a claim that slips through here is given up within one renewal interval rather than
// running beside another writer for the life of the process.
func (fl FileLock) takeOverExpired() bool {
	if err := fl.writeClaim(); err != nil {
		log.Err(err).Msgf("failed while claiming expired lock file %s", fl.lockFile)
		return false
	}

	time.Sleep(fl.claimSettle())

	owner, err := fl.readOwner()
	if err != nil {
		log.Err(err).Msgf("failed while confirming the claim on lock file %s", fl.lockFile)
		return false
	}
	if owner != fl.uniqueId {
		log.Info().Msgf("lock file %s went to [%s] while this instance was claiming it", fl.lockFile, owner)
		return false
	}
	return true
}

// claimSettle is how long a takeover waits before confirming itself. It is a fraction of the
// lease because the lease is the operator's own statement of how slow this filesystem might
// be: the pause only has to outlast the gap between two instances deciding the same lease has
// expired, and it is paid on a failover rather than on an ordinary start.
func (fl FileLock) claimSettle() time.Duration {
	return fl.leaseTime / 10
}

func (fl FileLock) reclaimer() {
	for {
		select {
		case <-fl.stop:
			return
		case <-time.After(fl.reclaimLease):
		}
		renewed, reason := fl.renewLease()
		if renewed {
			continue
		}
		if reason != nil {
			fl.lost(reason)
		}
		return
	}
}

// lost stops renewing and hands the reason to whoever asked to be told. It runs outside the
// renewal lock, so the callback cannot deadlock against a release.
func (fl FileLock) lost(reason error) {
	fl.stopOnce.Do(func() { close(fl.stop) })
	fl.onLost(reason)
}

// renewLease extends the lease by rewriting the lock file. It reports whether the lease is
// still held, and why it is not: a nil reason means the lock was released and there is nothing
// wrong, and a reason means this instance can no longer prove it holds the lease.
//
// It proves the lease is still ours before extending it. Without that check, a process that
// stalled past its lease — a long garbage collection pause, a frozen scheduler, a slow disk —
// woke up and truncated the lock file back to its own id, taking the lease from whoever had
// legitimately claimed it in the meantime. Both instances then believed they held it, and two
// writers on one log is the thing this lock exists to prevent.
//
// A filesystem has no compare-and-swap, so this is a read and then a write rather than an
// atomic claim. It closes the window a stall opens; it does not make renewal atomic.
func (fl FileLock) renewLease() (bool, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	select {
	case <-fl.stop:
		return false, nil
	default:
	}
	if reason := fl.stillOurs(); reason != nil {
		return false, reason
	}
	if err := fl.writeClaim(); err != nil {
		return false, errore.WrapWithContextF(err, "unable to write %s to renew the lease", fl.lockFile)
	}
	return true, nil
}

// writeClaim puts this instance's id in the lock file in one step, by writing a temporary file
// beside it and renaming that into place. A rename replaces the file atomically, so a reader
// sees either the old holder or the new one and never a half-written name.
//
// That matters more than it looks. Every decision here is made by reading this file, and the
// truncate-then-write it replaces left a window where a reader saw an empty file or a short
// read: a holder renewing its own lease would read that as having lost it, and an instance
// checking whether a lease was live would read it as expired and try to take over.
func (fl FileLock) writeClaim() error {
	temp := fl.lockFile + ".claim-" + fl.uniqueId
	file, err := fl.afero.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	_, writeErr := file.Write([]byte(fl.uniqueId))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		fl.removeQuietly(temp)
		return errore.WrapError(closeErr, writeErr)
	}
	if err = fl.afero.Rename(temp, fl.lockFile); err != nil {
		fl.removeQuietly(temp)
		return err
	}
	return nil
}

func (fl FileLock) removeQuietly(name string) {
	if err := fl.afero.Remove(name); err != nil {
		log.Err(err).Msgf("unable to remove the temporary lock file %s", name)
	}
}

// stillOurs says why the lease can no longer be proven to be held, or nil if it can.
//
// The file naming this instance is not enough. A lease that has already aged out can be
// claimed by anyone at any moment, so an instance whose own lease expired has lost it even if
// nobody has taken it yet. Stopping before the lease could be granted elsewhere is what a
// time-bounded lease is for; waiting to be told is what leaves two writers running.
func (fl FileLock) stillOurs() error {
	owner, err := fl.readOwner()
	if err != nil {
		return errore.WrapWithContextF(err, "unable to read the lock file %s", fl.lockFile)
	}
	if owner != fl.uniqueId {
		return errore.NewF("lock file %s is held by [%s], not by this instance [%s]",
			fl.lockFile, owner, fl.uniqueId)
	}
	modified, err := fl.getFileModificationTime()
	if err != nil {
		return errore.WrapWithContextF(err, "unable to read the age of the lock file %s", fl.lockFile)
	}
	if age := time.Since(modified); age >= fl.leaseTime {
		return errore.NewF("the lease on %s is %s old, past the %s it is granted for",
			fl.lockFile, age, fl.leaseTime)
	}
	return nil
}

// readOwner is the id in the lock file, which is whose lease it is.
func (fl FileLock) readOwner() (string, error) {
	content, err := fl.afero.ReadFile(fl.lockFile)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (fl FileLock) getFileModificationTime() (time.Time, error) {
	stat, err := fl.afero.Stat(fl.lockFile)
	if err != nil {
		return time.Time{}, err
	}
	return stat.ModTime(), nil
}

// ReleaseLock stops renewing and removes the lock file, but only one this instance still
// holds: after a lease is lost the file names whoever took it, and removing that would hand
// the log to a third writer. The same rule as renewal — never act on a lock you cannot prove
// is yours.
func (fl FileLock) ReleaseLock() bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.stopOnce.Do(func() { close(fl.stop) })
	owner, err := fl.readOwner()
	if err != nil {
		log.Err(err).Msgf("failed while reading lock file [%s] to release it", fl.lockFile)
		return false
	}
	if owner != fl.uniqueId {
		log.Warn().Msgf("not removing lock file [%s]: it is held by [%s], not by this instance [%s]",
			fl.lockFile, owner, fl.uniqueId)
		return false
	}
	err = fl.afero.Remove(fl.lockFile)
	if err != nil {
		log.Err(err).Msgf("failed to remove lock file [%s]", fl.lockFile)
		return false
	}
	log.Info().Msgf("Removed lockfile with id [%s]", fl.uniqueId)
	return true
}

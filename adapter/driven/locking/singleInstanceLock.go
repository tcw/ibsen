package locking

import (
	"io"
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

// Does file exist?
//	true -> is uuid owner?
//		true -> extend lease claim
//		false -> has lease expired?
//			true -> claim lock with new uuid (start lease update)
//			false -> backoff
// false -> claim lock with new uuid (start lease update)

func (fl FileLock) AcquireLock() bool {
	exists, err := fl.afero.Exists(fl.lockFile)
	if err != nil {
		log.Err(err).Msgf("failed while checking if file %s exists", fl.lockFile)
		return false
	}
	if exists {
		fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_RDONLY, 0550)
		if err != nil {
			log.Err(err).Msgf("failed while opening lock file %s", fl.lockFile)
			return false
		}
		byteUUID, err := io.ReadAll(fileLock)
		if err != nil {
			ioErr := fileLock.Close()
			if ioErr != nil {
				log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Err(err).Msgf("failed while reading lock file %s", fl.lockFile)
			return false
		}
		ioErr := fileLock.Close()
		if ioErr != nil {
			log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		lockUUID := string(byteUUID)
		if lockUUID != fl.uniqueId {
			modificationTime, err := fl.getFileModificationTime()
			if err != nil {
				log.Err(err).Msgf("unable to get modification time for file %s", fl.lockFile)
				return false
			}
			if modificationTime.Add(fl.leaseTime).Before(time.Now()) {
				fileLockAdder, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_TRUNC, 0660)
				if err != nil {
					log.Err(err).Msgf("failed while opening lock file %s", fl.lockFile)
					return false
				}
				_, err = fileLockAdder.Write([]byte(fl.uniqueId))
				ioErr := fileLockAdder.Close()
				if err != nil || ioErr != nil {
					log.Err(errore.WrapError(ioErr, err)).Msgf("failed while claiming expired lock file %s", fl.lockFile)
					return false
				}
				go fl.reclaimer()
				return true
			} else {
				return false
			}
		}
	} else {
		fileLockNew, err := fl.afero.OpenFile(fl.lockFile, os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			log.Err(err).Msgf("failed while claiming lock file %s", fl.lockFile)
			return false
		}
		_, err = fileLockNew.Write([]byte(fl.uniqueId))
		if err != nil {
			ioErr := fileLockNew.Close()
			if ioErr != nil {
				log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Err(err).Msgf("failed while writing to claim lock file %s", fl.lockFile)
			return false
		}
		go fl.reclaimer()
		ioErr := fileLockNew.Close()
		if ioErr != nil {
			log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		return true
	}
	return false
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
	// the lock file must already exist: O_EXCL without O_CREATE is ignored by Linux but
	// rejects an existing file on other filesystems, so it is not used here
	fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		return false, errore.WrapWithContextF(err, "unable to open %s to renew the lease", fl.lockFile)
	}
	_, err = fileLock.Write([]byte(fl.uniqueId))
	ioErr := fileLock.Close()
	if err != nil {
		return false, errore.WrapWithContextF(err, "unable to write %s to renew the lease", fl.lockFile)
	}
	if ioErr != nil {
		log.Printf("failed while closing lock file %s", fl.lockFile)
	}
	return true, nil
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

package wiring

import (
	"errors"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/adapter/driven/locking/flock"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

// The single-writer locks a data directory can be guarded by, named by --lock.
const (
	// LockAuto takes a kernel lock where one is guaranteed and the lease file otherwise,
	// and says which it chose and why.
	LockAuto = "auto"
	// LockFlock takes a kernel lock, and refuses to start where one is not guaranteed.
	LockFlock = "flock"
	// LockFile takes the renewed lease file the server has always used.
	LockFile = "file"
	// LockNone takes nothing: for a directory nothing else will ever open.
	LockNone = "none"
)

// DefaultLock is what a deployment that says nothing gets.
const DefaultLock = LockAuto

// LockModes is every name --lock accepts, in the order the help lists them.
var LockModes = []string{LockAuto, LockFlock, LockFile, LockNone}

// ErrUnknownLock is returned for a --lock name with no adapter behind it. It is refused
// rather than replaced, since a guess at how a log is guarded is the wrong kind of guess.
var ErrUnknownLock = errors.New("unknown write lock")

// ErrLockNotGuaranteed is returned when --lock flock was asked for where a kernel lock cannot
// be relied on. The error names the reason: no flock on this platform, a network or unknown
// filesystem, or a filesystem that granted the lock twice.
var ErrLockNotGuaranteed = errors.New("a kernel file lock is not guaranteed for this data directory")

// buildWriteLock is the adapter behind a --lock name, for the data directory root. onLost is
// what the lease file does when it can no longer prove it holds the lease; a kernel lock has
// no such path, because it cannot be lost while the process lives.
func buildWriteLock(mode, root string, onLost locking.LeaseLost) (driven.SingleIbsenWriterLock, error) {
	path := filepath.Join(root, writeLockFileName)
	switch mode {
	case "", LockAuto:
		return &autoLock{root: root, onLost: onLost}, nil
	case LockFlock:
		if err := kernelLockGuaranteed(root); err != nil {
			return nil, errore.WrapWithContextF(ErrLockNotGuaranteed, "%v", err)
		}
		return flock.New(path), nil
	case LockFile:
		return leaseFile(root, onLost), nil
	case LockNone:
		return driven.NoFileLock{}, nil
	default:
		return nil, errore.WrapWithContextF(ErrUnknownLock, "%q is not one of %v", mode, LockModes)
	}
}

func leaseFile(root string, onLost locking.LeaseLost) locking.FileLock {
	return locking.NewFileLock(filepath.Join(root, writeLockFileName), writeLockLease, writeLockReclaim, onLost)
}

// acquirer is a lock that can say why it was refused, which AcquireLock's bool cannot. The
// port stays as narrow as it is; the composition root asks the adapters it built.
type acquirer interface {
	Acquire() error
}

// acquireWriteLock takes the lock, and says why it could not: ErrWriteLockUnavailable when
// somebody else holds it, ErrLockNotGuaranteed when a kernel lock was asked for and the
// filesystem would not honour it.
func acquireWriteLock(lock driven.SingleIbsenWriterLock, root string) error {
	if explained, ok := lock.(acquirer); ok {
		err := explained.Acquire()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrWriteLockUnavailable), errors.Is(err, flock.ErrHeld):
			return errore.WrapWithContextF(ErrWriteLockUnavailable,
				"unable to acquire the single writer lock on path [%s]: %v", root, err)
		case errors.Is(err, flock.ErrNotGuaranteed):
			return errore.WrapWithContextF(ErrLockNotGuaranteed, "%v", err)
		default:
			return errore.WrapWithContextF(err, "unable to acquire the single writer lock on path [%s]", root)
		}
	}
	if !lock.AcquireLock() {
		return errore.WrapWithContextF(ErrWriteLockUnavailable,
			"unable to acquire the single writer lock on path [%s]", root)
	}
	return nil
}

// autoLock is --lock auto: a kernel lock where the platform, the filesystem and a self-test
// all say it holds, and the lease file everywhere else. It decides when it is acquired rather
// than when it is built, since the self-test needs the lock held to run.
type autoLock struct {
	root   string
	onLost locking.LeaseLost
	chosen driven.SingleIbsenWriterLock
}

var _ driven.SingleIbsenWriterLock = &autoLock{}

// kernelLock is what auto needs of the flock adapter.
type kernelLock interface {
	driven.SingleIbsenWriterLock
	acquirer
}

// kernelLockGuaranteed and newKernelLock are variables so a test can put the data directory
// on NFS, or on a filesystem that grants a kernel lock twice.
var (
	kernelLockGuaranteed = flock.Guaranteed
	newKernelLock        = func(path string) kernelLock { return flock.New(path) }
)

func (a *autoLock) Acquire() error {
	if err := kernelLockGuaranteed(a.root); err != nil {
		log.Info().Msgf("write lock: using the lease file, because %v", err)
		return a.useLeaseFile()
	}
	kernel := newKernelLock(filepath.Join(a.root, writeLockFileName))
	err := kernel.Acquire()
	switch {
	case err == nil:
		log.Info().Msgf("write lock: using a kernel lock (flock) on %s", filepath.Join(a.root, writeLockFileName))
		a.chosen = kernel
		return nil
	case errors.Is(err, flock.ErrNotGuaranteed):
		log.Warn().Msgf("write lock: using the lease file, because %v", err)
		return a.useLeaseFile()
	default:
		return err
	}
}

func (a *autoLock) useLeaseFile() error {
	lease := leaseFile(a.root, a.onLost)
	if !lease.AcquireLock() {
		return ErrWriteLockUnavailable
	}
	a.chosen = lease
	return nil
}

func (a *autoLock) AcquireLock() bool { return a.Acquire() == nil }

func (a *autoLock) ReleaseLock() bool {
	if a.chosen == nil {
		return false
	}
	return a.chosen.ReleaseLock()
}

// Chosen is the lock auto settled on, or nil before it is acquired.
func (a *autoLock) Chosen() driven.SingleIbsenWriterLock { return a.chosen }

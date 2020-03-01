package filelock

import (
	"errors"
	"github.com/tcw/ibsen/filelock"
	"os"
)

// A File provides the minimal set of methods required to lock an open file.
// File implementations must be usable as map keys.
// The usual implementation is *os.File.
type File interface {
	// Name returns the name of the file.
	Name() string

	// Fd returns a valid file descriptor.
	// (If the File is an *os.File, it must not be closed.)
	Fd() uintptr

	// Stat returns the FileInfo structure describing file.
	Stat() (os.FileInfo, error)
}

// Lock places an advisory write lock on the file, blocking until it can be
// locked.
//
// If Lock returns nil, no other process will be able to place a read or write
// lock on the file until this process exits, closes f, or calls Unlock on it.
//
// If f's descriptor is already read- or write-locked, the behavior of Lock is
// unspecified.
//
// Closing the file may or may not release the lock promptly. Callers should
// ensure that Unlock is always called when Lock succeeds.
func Lock(f File) error {
	return filelock.lock(f, filelock.writeLock)
}

// RLock places an advisory read lock on the file, blocking until it can be locked.
//
// If RLock returns nil, no other process will be able to place a write lock on
// the file until this process exits, closes f, or calls Unlock on it.
//
// If f is already read- or write-locked, the behavior of RLock is unspecified.
//
// Closing the file may or may not release the lock promptly. Callers should
// ensure that Unlock is always called if RLock succeeds.
func RLock(f File) error {
	return filelock.lock(f, filelock.readLock)
}

// Unlock removes an advisory lock placed on f by this process.
//
// The caller must not attempt to unlock a file that is not locked.
func Unlock(f File) error {
	return filelock.unlock(f)
}

// String returns the name of the function corresponding to lt
// (Lock, RLock, or Unlock).
func (lt filelock.lockType) String() string {
	switch lt {
	case filelock.readLock:
		return "RLock"

	case filelock.writeLock:
		return "Lock"

	default:
		return "Unlock"
	}
}

// IsNotSupported returns a boolean indicating whether the error is known to
// report that a function is not supported (possibly for a specific input).
// It is satisfied by ErrNotSupported as well as some syscall errors.
func IsNotSupported(err error) bool {
	return filelock.isNotSupported(underlyingError(err))
}

var ErrNotSupported = errors.New("operation not supported")

// underlyingError returns the underlying error for known os error types.
func underlyingError(err error) error {
	switch err := err.(type) {
	case *os.PathError:
		return err.Err
	case *os.LinkError:
		return err.Err
	case *os.SyscallError:
		return err.Err
	}
	return err
}

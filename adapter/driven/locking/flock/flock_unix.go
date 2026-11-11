//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flock

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Supported says this platform has flock.
const Supported = true

// Lock holds an exclusive kernel lock on one file for the life of the process.
type Lock struct {
	path string
	id   string

	mu   sync.Mutex
	file *os.File
}

// New returns a lock on path, not yet taken.
func New(path string) *Lock {
	var random [16]byte
	_, _ = rand.Read(random[:])
	return &Lock{path: path, id: "flock-" + hex.EncodeToString(random[:])}
}

// ID is what this lock writes into the file once it holds it, so a person looking at the file
// sees who holds it and a lease-file instance reads it as somebody else's.
func (l *Lock) ID() string { return l.id }

// AcquireLock satisfies driven.SingleIbsenWriterLock. Acquire says why it failed.
func (l *Lock) AcquireLock() bool {
	return l.Acquire() == nil
}

// Acquire takes the lock, or says why it did not: ErrHeld when another holder has it, and
// ErrNotGuaranteed when this filesystem was found not to enforce it.
//
// The lock is taken on whatever inode the path names at that moment, and a holder that
// releases removes the file, so a lock taken on an inode somebody has just unlinked would be a
// lock on nothing. It is checked: after flock succeeds, the open descriptor and the path must
// still be the same file, and if they are not, the attempt starts again on the new one.
func (l *Lock) Acquire() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return fmt.Errorf("%w: this instance already holds %s", ErrHeld, l.path)
	}
	for attempt := 0; attempt < 10; attempt++ {
		file, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			return err
		}
		if err := flockNB(file); err != nil {
			file.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return fmt.Errorf("%w: %s is locked by another process", ErrHeld, l.path)
			}
			return fmt.Errorf("unable to lock %s: %w", l.path, err)
		}
		same, err := stillThePath(file, l.path)
		if err != nil {
			file.Close()
			return err
		}
		if !same {
			// the previous holder removed it between our open and our lock: try the new one
			file.Close()
			continue
		}
		if err := l.claim(file); err != nil {
			if errors.Is(err, ErrNotGuaranteed) {
				// whatever takes over from here, the lease file most likely, must not find an
				// empty file we created and read it as a holder that has just written nothing
				removeIfEmpty(file, l.path)
			}
			file.Close()
			return err
		}
		l.file = file
		return nil
	}
	return fmt.Errorf("%w: %s kept being replaced while it was being locked", ErrHeld, l.path)
}

// claim finishes taking a file the kernel has just locked for us: it runs the self-test,
// refuses a live lease a lease-file instance holds in it, and writes our id.
func (l *Lock) claim(file *os.File) error {
	refused, err := secondHolderRefused(l.path)
	if err != nil {
		return err
	}
	if !refused {
		return fmt.Errorf("%w: a second descriptor on %s was granted the lock this one holds, "+
			"so the filesystem is not enforcing it", ErrNotGuaranteed, l.path)
	}
	if holder, live := liveLease(file); live {
		return fmt.Errorf("%w: %s holds a live lease in %s", ErrHeld, holder, l.path)
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.WriteAt([]byte(l.id), 0); err != nil {
		return err
	}
	return nil
}

// liveLease reports a holder the lease-file adapter wrote and is still renewing. A kernel lock
// says nothing about an instance that only knows the lease file, so without this a flock
// instance would walk straight past one. A file we wrote ourselves, or one not touched for a
// lease, is not a live lease.
func liveLease(file *os.File) (string, bool) {
	info, err := file.Stat()
	if err != nil || info.Size() == 0 || info.Size() > 256 {
		return "", false
	}
	content := make([]byte, info.Size())
	if _, err := file.ReadAt(content, 0); err != nil {
		return "", false
	}
	holder := string(content)
	if len(holder) >= 6 && holder[:6] == "flock-" {
		// a flock holder that died: the kernel released it, which is why we hold it now
		return "", false
	}
	return holder, time.Since(info.ModTime()) < LeaseTime
}

// ReleaseLock removes the file and lets the lock go, in that order: the file is removed
// while it is still locked, so nobody can lock the inode it named after we let go of it and
// believe they hold the lock on a file that is no longer there. A process that dies instead
// leaves the file, and the kernel drops its lock.
func (l *Lock) ReleaseLock() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return false
	}
	removed := os.Remove(l.path) == nil
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return removed && unlockErr == nil && closeErr == nil
}

func removeIfEmpty(file *os.File, path string) {
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		_ = os.Remove(path)
	}
}

func flockNB(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err != syscall.EINTR {
			return err
		}
	}
}

func stillThePath(file *os.File, path string) (bool, error) {
	held, err := file.Stat()
	if err != nil {
		return false, err
	}
	named, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return os.SameFile(held, named), nil
}

// secondHolderRefused is the self-test: with the lock held, a second, independent open of the
// same file must be refused it. flock locks belong to an open file description, so this holds
// within one process on any filesystem that enforces them. A variable so a test can play a
// filesystem that does not.
var secondHolderRefused = func(path string) (bool, error) {
	second, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("self-test: unable to open %s a second time: %w", path, err)
	}
	defer second.Close()
	err = flockNB(second)
	if err == nil {
		_ = syscall.Flock(int(second.Fd()), syscall.LOCK_UN)
		return false, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return true, nil
	}
	return false, fmt.Errorf("self-test: unexpected error locking %s a second time: %w", path, err)
}

// Guaranteed says whether a kernel lock on a file in dir can be relied on: nil if it can, and
// an error wrapping ErrNotGuaranteed that names the reason if it cannot.
func Guaranteed(dir string) error {
	fs, err := filesystemOf(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("%w: unable to tell what filesystem %s is on: %v", ErrNotGuaranteed, dir, err)
	}
	if !fs.Local {
		reason := "it is not on the list of local filesystems known to honour flock"
		if fs.Remote {
			reason = "it is a network or remote filesystem, where a lock may hold on one machine only or not at all"
		}
		return fmt.Errorf("%w: %s is on %s, and %s", ErrNotGuaranteed, dir, fs.Name, reason)
	}
	return nil
}

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package locking

import (
	"os"
	"syscall"
)

// heldByKernelLock reports whether an instance using the flock adapter holds the lock file.
// That instance renews nothing, so the file's age says nothing about whether it is alive; the
// kernel lock does. The check takes the lock for an instant and lets it go, which at worst
// makes a flock instance starting at that same instant find it held and refuse, the safe way
// round.
func heldByKernelLock(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		return false
	}
	return err == syscall.EWOULDBLOCK
}

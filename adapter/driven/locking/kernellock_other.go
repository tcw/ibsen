//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package locking

// heldByKernelLock is always false where there is no flock, since no instance here can hold
// one.
func heldByKernelLock(path string) bool { return false }

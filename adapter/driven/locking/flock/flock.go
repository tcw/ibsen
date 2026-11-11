// Package flock is the single-writer lock held by the kernel: flock(2) on <root>/.writeLock.
//
// It is what the lease file would be if a filesystem had a compare-and-swap. The kernel grants
// the lock atomically, holds it for as long as the process keeps the file open — a process
// frozen for a minute still holds it, so nobody can take over while it is stalled — and
// releases it the moment the process dies, however it dies. There is no renewal, so there is
// no stall window and nothing to lose: the adapter has no lease-lost path because the lock
// cannot be lost while the process lives.
//
// It is only as good as the filesystem honouring it, which is why it is used only where that
// can be checked. Guaranteed says whether the platform has flock and the data directory is on
// a local filesystem known to honour it; Acquire then proves it on the spot, by taking the lock
// on a second, independent descriptor and expecting to be refused. A network filesystem may
// accept flock and enforce it on one machine only, or not at all, and that is the failure this
// package exists to refuse rather than discover.
//
// Standard library and syscall only.
package flock

import (
	"errors"
	"time"
)

// ErrHeld is returned by Acquire when another holder has the lock: another process holding
// the kernel lock, or an instance holding a live lease in the same file.
var ErrHeld = errors.New("the write lock is held by another instance")

// ErrNotGuaranteed is returned when a kernel lock cannot be relied on here: the platform has
// no flock, the filesystem is remote or of a type not known to honour it, or the self-test
// found the lock not being enforced. The error says which.
var ErrNotGuaranteed = errors.New("a kernel file lock is not guaranteed here")

// LeaseTime is how long a lease written by the lease-file adapter stays live after its last
// renewal. A kernel lock found on a file whose content names a holder renewed more recently
// than this is a lease-file instance still holding it, and is refused. It matches the lease
// the server and OpenLocal grant.
var LeaseTime = 10 * time.Second

package wiring

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/adapter/driven/locking/flock"
	"github.com/tcw/ibsen/core/port/driven"
)

func failOnLost(t *testing.T) locking.LeaseLost {
	return func(reason error) { t.Errorf("lease lost: %v", reason) }
}

func onNFS(t *testing.T) {
	t.Helper()
	saved := kernelLockGuaranteed
	t.Cleanup(func() { kernelLockGuaranteed = saved })
	kernelLockGuaranteed = func(dir string) error {
		return fmt.Errorf("%w: %s is on nfs, and it is a network or remote filesystem", flock.ErrNotGuaranteed, dir)
	}
}

func localDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := flock.Guaranteed(dir); err != nil {
		t.Skipf("no kernel lock here: %v", err)
	}
	return dir
}

func TestEachLockNameBuildsItsAdapter(t *testing.T) {
	root := t.TempDir()
	cases := map[string]string{
		"":        "*wiring.autoLock",
		LockAuto:  "*wiring.autoLock",
		LockFile:  "locking.FileLock",
		LockNone:  "driven.NoFileLock",
		LockFlock: "*flock.Lock",
	}
	for mode, want := range cases {
		if mode == LockFlock && flock.Guaranteed(root) != nil {
			continue
		}
		lock, err := buildWriteLock(mode, root, failOnLost(t))
		if err != nil {
			t.Fatalf("%q: %v", mode, err)
		}
		if got := fmt.Sprintf("%T", lock); got != want {
			t.Errorf("--lock %q built %s, want %s", mode, got, want)
		}
	}
}

func TestAnUnknownLockNameIsRefused(t *testing.T) {
	_, err := buildWriteLock("etcd", t.TempDir(), failOnLost(t))
	if !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v, want ErrUnknownLock", err)
	}
}

func TestAutoTakesAKernelLockOnALocalFilesystem(t *testing.T) {
	root := localDir(t)
	lock, _ := buildWriteLock(LockAuto, root, failOnLost(t))
	if err := acquireWriteLock(lock, root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.ReleaseLock() })
	if _, isKernel := lock.(*autoLock).Chosen().(*flock.Lock); !isKernel {
		t.Errorf("auto chose %T on a local filesystem, want the kernel lock", lock.(*autoLock).Chosen())
	}
}

// What a platform with no flock, or a data directory on a network filesystem, gets.
func TestAutoFallsBackToTheLeaseFileWhereFlockIsNotGuaranteed(t *testing.T) {
	onNFS(t)
	root := t.TempDir()
	lock, _ := buildWriteLock(LockAuto, root, failOnLost(t))
	if err := acquireWriteLock(lock, root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.ReleaseLock() })
	if _, isLease := lock.(*autoLock).Chosen().(locking.FileLock); !isLease {
		t.Errorf("auto chose %T on nfs, want the lease file", lock.(*autoLock).Chosen())
	}
}

// selfTestFails is a filesystem that says it is local and then grants the lock twice.
type selfTestFails struct{}

func (selfTestFails) Acquire() error {
	return fmt.Errorf("%w: a second descriptor was granted the lock", flock.ErrNotGuaranteed)
}
func (selfTestFails) AcquireLock() bool { return false }
func (selfTestFails) ReleaseLock() bool { return false }

func TestAutoFallsBackWhenTheSelfTestFails(t *testing.T) {
	saved := newKernelLock
	t.Cleanup(func() { newKernelLock = saved })
	newKernelLock = func(string) kernelLock { return selfTestFails{} }

	root := t.TempDir()
	lock, _ := buildWriteLock(LockAuto, root, failOnLost(t))
	if err := acquireWriteLock(lock, root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.ReleaseLock() })
	if _, isLease := lock.(*autoLock).Chosen().(locking.FileLock); !isLease {
		t.Errorf("auto chose %T after a failed self-test, want the lease file", lock.(*autoLock).Chosen())
	}
}

// Explicit flock never quietly becomes something else.
func TestExplicitFlockRefusesWithTheReason(t *testing.T) {
	onNFS(t)
	_, err := buildWriteLock(LockFlock, t.TempDir(), failOnLost(t))
	if !errors.Is(err, ErrLockNotGuaranteed) {
		t.Fatalf("got %v, want ErrLockNotGuaranteed", err)
	}
	if !strings.Contains(err.Error(), "nfs") {
		t.Errorf("the refusal does not name the reason: %v", err)
	}
}

func TestExplicitFlockRefusesWhenTheSelfTestFails(t *testing.T) {
	err := acquireWriteLock(selfTestFails{}, t.TempDir())
	if !errors.Is(err, ErrLockNotGuaranteed) {
		t.Fatalf("got %v, want ErrLockNotGuaranteed", err)
	}
}

func TestAHeldKernelLockIsTheUsualRefusal(t *testing.T) {
	root := localDir(t)
	first := flock.New(filepath.Join(root, writeLockFileName))
	if err := first.Acquire(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.ReleaseLock() })
	for _, mode := range []string{LockAuto, LockFlock} {
		lock, _ := buildWriteLock(mode, root, failOnLost(t))
		if err := acquireWriteLock(lock, root); !errors.Is(err, ErrWriteLockUnavailable) {
			t.Errorf("--lock %s got %v while another holds the lock, want ErrWriteLockUnavailable", mode, err)
		}
	}
}

// Mixed deployments: an instance on the lease file and one on the kernel lock see each other.
func TestALeaseFileInstanceRefusesAFlockHeldDirectory(t *testing.T) {
	root := localDir(t)
	kernel := flock.New(filepath.Join(root, writeLockFileName))
	if err := kernel.Acquire(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kernel.ReleaseLock() })

	// a kernel lock renews nothing, so to a lease file it looks long expired
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(root, writeLockFileName), old, old); err != nil {
		t.Fatal(err)
	}
	lease := leaseFile(root, failOnLost(t))
	if lease.AcquireLock() {
		lease.ReleaseLock()
		t.Fatal("a lease-file instance took a directory a kernel lock holds")
	}
}

func TestAFlockInstanceRefusesALiveLeaseFile(t *testing.T) {
	root := localDir(t)
	lease := leaseFile(root, failOnLost(t))
	if !lease.AcquireLock() {
		t.Fatal("the lease file could not be taken")
	}
	t.Cleanup(func() { lease.ReleaseLock() })

	kernel := flock.New(filepath.Join(root, writeLockFileName))
	if err := kernel.Acquire(); !errors.Is(err, flock.ErrHeld) {
		kernel.ReleaseLock()
		t.Fatalf("got %v over a live lease file, want flock.ErrHeld", err)
	}
}

func TestNoneTakesNothing(t *testing.T) {
	root := t.TempDir()
	lock, _ := buildWriteLock(LockNone, root, failOnLost(t))
	if _, ok := lock.(driven.NoFileLock); !ok {
		t.Fatalf("built %T", lock)
	}
	if err := acquireWriteLock(lock, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, writeLockFileName)); !os.IsNotExist(err) {
		t.Errorf("--lock none touched the lock file: %v", err)
	}
}

func TestOpenLocalTakesTheLockItIsTold(t *testing.T) {
	root := localDir(t)
	first, err := OpenLocal(LocalParams{RootPath: root, LockMode: LockFlock, Compression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenLocal(LocalParams{RootPath: root, LockMode: LockFile, Compression: "none"})
	if !errors.Is(err, ErrWriteLockUnavailable) {
		t.Errorf("a lease-file open beside a flock open got %v, want ErrWriteLockUnavailable", err)
	}
	first.Close()

	_, err = OpenLocal(LocalParams{RootPath: root, LockMode: "bogus"})
	if !errors.Is(err, ErrUnknownLock) {
		t.Errorf("got %v, want ErrUnknownLock", err)
	}
}

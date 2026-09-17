package wiring

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
)

// The CLI used to build the lease itself, at <root>/.writeLock. The composition root now
// does, and it has to land in the same place: an operator's existing data directory already
// holds one, and another instance is fenced off by that exact path.
func TestStartBuildsTheWriteLockInTheDataDirectory(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	ibs := &IbsenServer{Afs: afs, RootPath: "/data"}

	ibs.defaults()

	if ibs.Lock == nil {
		t.Fatal("no lock was built for a server that was given none")
	}
	if !ibs.Lock.AcquireLock() {
		t.Fatal("the lock that was built could not be acquired")
	}
	t.Cleanup(func() { ibs.Lock.ReleaseLock() })

	exists, err := afs.Exists("/data/.writeLock")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("acquiring the lock did not create /data/.writeLock")
	}
}

// A caller that brings its own coordination adapter keeps it; defaults only fills gaps.
func TestAnInjectedLockIsKept(t *testing.T) {
	injected := &slowReleaseLock{released: make(chan struct{})}
	ibs := &IbsenServer{
		Afs:      &afero.Afero{Fs: afero.NewMemMapFs()},
		RootPath: "/data",
		Lock:     injected,
	}

	ibs.defaults()

	if ibs.Lock != injected {
		t.Errorf("defaults replaced an injected lock with %T", ibs.Lock)
	}
}

// refusingLock stands in for another instance already holding the lease.
type refusingLock struct{ acquireCalls int }

func (l *refusingLock) AcquireLock() bool {
	l.acquireCalls++
	return false
}

func (l *refusingLock) ReleaseLock() bool { return true }

// Start used to log.Fatal here, which exits the process from inside a library call: a
// program embedding the log had no say. It returns the refusal instead.
func TestStartReturnsWhenAnotherInstanceHoldsTheLock(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	lock := &refusingLock{}
	ibs := &IbsenServer{Lock: lock, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	started := make(chan error, 1)
	go func() { started <- ibs.Start(lis) }()

	select {
	case err := <-started:
		if err == nil {
			t.Fatal("Start returned no error although the lock was refused")
		}
		if !errors.Is(err, ErrWriteLockUnavailable) {
			t.Errorf("error does not match ErrWriteLockUnavailable: %v", err)
		}
		if !strings.Contains(err.Error(), "/data") {
			t.Errorf("error does not say which path it could not lock: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the lock was refused")
	}

	if lock.acquireCalls != 1 {
		t.Errorf("AcquireLock was called %d times, want 1", lock.acquireCalls)
	}
}

// A read-only server writes nothing, so it never takes the lease and a held lock does not
// stop it starting.
func TestReadonlyStartDoesNotTakeTheLock(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lock := &refusingLock{}
	ibs := &IbsenServer{Readonly: true, Lock: lock, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	started := make(chan error, 1)
	go func() { started <- ibs.Start(lis) }()

	// it got past the lock check if it is still serving
	select {
	case err := <-started:
		t.Fatalf("a read-only Start returned early: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	ibs.ShutdownCleanly()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after a clean shutdown")
	}

	if lock.acquireCalls != 0 {
		t.Errorf("a read-only server took the write lock %d times", lock.acquireCalls)
	}
}

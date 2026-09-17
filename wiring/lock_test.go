package wiring

import (
	"testing"

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

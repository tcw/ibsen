package locking

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
)

// forEachFs runs test against an in-memory and an OS filesystem, whose open flags differ.
func forEachFs(t *testing.T, test func(t *testing.T, afs *afero.Afero, lockFile string)) {
	t.Run("mem", func(t *testing.T) {
		test(t, &afero.Afero{Fs: afero.NewMemMapFs()}, "lock")
	})
	t.Run("os", func(t *testing.T) {
		test(t, &afero.Afero{Fs: afero.NewOsFs()}, filepath.Join(t.TempDir(), "lock"))
	})
}

func TestFileLock_renewsLeaseWhileHeld(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		lock := NewFileLock(afs, lockFile, time.Second, 5*time.Millisecond)
		if !lock.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}
		defer lock.ReleaseLock()
		claimed, err := lock.getFileModificationTime()
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			renewed, err := lock.getFileModificationTime()
			if err != nil {
				t.Fatal(err)
			}
			if renewed.After(claimed) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("lease was not renewed")
	})
}

func TestFileLock_releaseStopsRenewingLease(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		lock := NewFileLock(afs, lockFile, time.Second, time.Millisecond)
		if !lock.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}
		if !lock.ReleaseLock() {
			t.Fatal("unable to release the lock")
		}
		// a renewal after release used to open the removed lock file and crash
		time.Sleep(50 * time.Millisecond)
		if exists, _ := afs.Exists(lockFile); exists {
			t.Fatal("lock file was recreated after release")
		}
	})
}

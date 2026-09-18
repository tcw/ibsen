package locking

import (
	"path/filepath"
	"strings"
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
		lock := NewFileLock(afs, lockFile, time.Second, 5*time.Millisecond, failOnLoss(t))
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
		lock := NewFileLock(afs, lockFile, time.Second, time.Millisecond, failOnLoss(t))
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

// failOnLoss is for the tests that expect the lease to be held throughout.
func failOnLoss(t *testing.T) LeaseLost {
	return func(reason error) {
		t.Errorf("the lease was reported lost: %v", reason)
	}
}

// age moves the lock file's timestamps back, which is how a test makes a lease look stale
// without waiting for one.
func age(t *testing.T, afs *afero.Afero, lockFile string, by time.Duration) {
	t.Helper()
	when := time.Now().Add(-by)
	if err := afs.Chtimes(lockFile, when, when); err != nil {
		t.Fatal(err)
	}
}

func ownerOf(t *testing.T, afs *afero.Afero, lockFile string) string {
	t.Helper()
	content, err := afs.ReadFile(lockFile)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

// The bug this pins. A writer that stalls past its lease used to wake up and truncate the lock
// file back to its own id, taking the lease from the instance that had legitimately claimed
// it. Both then believed they held it, and two writers on one log is the thing this lock
// exists to prevent. It needs no network partition: one long pause on one machine does it.
func TestARenewalAfterTheLeaseExpiredDoesNotStealItBack(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		const lease = time.Second
		// a reclaim interval longer than the test, so renewal happens only when asked for
		stalled := NewFileLock(afs, lockFile, lease, time.Hour, failOnLoss(t))
		if !stalled.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}

		// the holder stalls long enough for its lease to expire
		age(t, afs, lockFile, 2*lease)

		// another instance takes the expired lease
		successor := NewFileLock(afs, lockFile, lease, time.Hour, failOnLoss(t))
		if !successor.AcquireLock() {
			t.Fatal("the successor could not claim the expired lease")
		}
		if ownerOf(t, afs, lockFile) != successor.uniqueId {
			t.Fatal("the successor did not take the lock file")
		}

		// the stalled instance wakes up and tries to renew
		renewed, reason := stalled.renewLease()

		if renewed {
			t.Error("the stalled instance renewed a lease it no longer held")
		}
		if reason == nil {
			t.Error("the stalled instance was not told why it could not renew")
		}
		if owner := ownerOf(t, afs, lockFile); owner != successor.uniqueId {
			t.Errorf("the lock file is now held by [%s], want the successor [%s]: the stalled instance stole it back",
				owner, successor.uniqueId)
		}
	})
}

// It is not enough to notice somebody else took the lease. An instance whose own lease has
// aged out has lost it whether or not anyone has claimed it yet, because anyone may claim it
// at any moment. Stopping before the lease could be granted elsewhere is what a time-bounded
// lease is for.
func TestARenewalStopsOnceItsOwnLeaseHasAgedOut(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		const lease = time.Second
		lock := NewFileLock(afs, lockFile, lease, time.Hour, failOnLoss(t))
		if !lock.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}

		// nobody has taken it; this instance simply stalled past its own lease
		age(t, afs, lockFile, 2*lease)

		renewed, reason := lock.renewLease()

		if renewed {
			t.Error("an instance renewed a lease that had already expired")
		}
		if reason == nil {
			t.Fatal("no reason was given for refusing to renew")
		}
		if !strings.Contains(reason.Error(), "past the") {
			t.Errorf("reason is %q, which does not say the lease had aged out", reason)
		}
	})
}

// Losing the lease tells whoever asked to be told, and stops the renewals rather than carrying
// on regardless. In the server that callback exits the process.
func TestLosingTheLeaseIsReportedAndStopsRenewing(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		lost := make(chan error, 4)
		lock := NewFileLock(afs, lockFile, 50*time.Millisecond, 5*time.Millisecond,
			func(reason error) { lost <- reason })
		if !lock.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}

		// somebody else takes the lock file out from under it
		thief := "another-instance"
		if err := afs.WriteFile(lockFile, []byte(thief), 0660); err != nil {
			t.Fatal(err)
		}

		select {
		case reason := <-lost:
			if reason == nil {
				t.Fatal("the lease was reported lost with no reason")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the renewer never noticed the lease was gone")
		}

		// and it renews no more: the thief's id stays put
		time.Sleep(50 * time.Millisecond)
		if owner := ownerOf(t, afs, lockFile); owner != thief {
			t.Errorf("the lock file is held by [%s] after the lease was lost, want [%s]", owner, thief)
		}
		if len(lost) != 0 {
			t.Errorf("the lease was reported lost %d more times; the renewer did not stop", len(lost))
		}
	})
}

// The same rule on the way out: a lock this instance no longer holds is not its to remove.
// Removing it would hand the log to a third writer while the second is still using it.
func TestReleaseDoesNotRemoveALockItNoLongerHolds(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		lock := NewFileLock(afs, lockFile, time.Second, time.Hour, failOnLoss(t))
		if !lock.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}
		successor := "another-instance"
		if err := afs.WriteFile(lockFile, []byte(successor), 0660); err != nil {
			t.Fatal(err)
		}

		if lock.ReleaseLock() {
			t.Error("releasing a lock held by somebody else reported success")
		}

		if exists, _ := afs.Exists(lockFile); !exists {
			t.Fatal("the successor's lock file was removed")
		}
		if owner := ownerOf(t, afs, lockFile); owner != successor {
			t.Errorf("the lock file is held by [%s], want the successor [%s]", owner, successor)
		}
	})
}

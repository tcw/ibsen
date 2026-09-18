package locking

import (
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// countWinners has instances race for the same lock file and reports how many came away
// believing they hold it. Anything but one is a bug: two is split-brain, and zero means a
// free lock nobody could take.
func countWinners(t *testing.T, afs *afero.Afero, lockFile string, lease time.Duration, racers int) int {
	t.Helper()
	locks := make([]FileLock, racers)
	for i := range locks {
		// a reclaim interval longer than the test, so nothing renews behind our back
		locks[i] = NewFileLock(afs, lockFile, lease, time.Hour, func(error) {})
	}
	start := make(chan struct{})
	var won atomic.Int64
	var wg sync.WaitGroup
	for i := range locks {
		wg.Add(1)
		go func(lock FileLock) {
			defer wg.Done()
			<-start
			if lock.AcquireLock() {
				won.Add(1)
			}
		}(locks[i])
	}
	close(start)
	wg.Wait()
	return int(won.Load())
}

// onRealFs runs a race against a real filesystem only.
//
// The in-memory one cannot answer these. afero's MemMapFs.OpenFile checks whether the file
// exists and then creates it under two separate locks, so O_CREATE|O_EXCL is not atomic there
// and two instances can both create the same lock file. That is afero's, not this adapter's,
// and it is not a deployment: the server takes this lock only when it is not running in
// memory. Running these against MemMapFs would be asserting a guarantee nothing can provide.
func onRealFs(t *testing.T, test func(t *testing.T, afs *afero.Afero, lockFile string)) {
	t.Helper()
	test(t, &afero.Afero{Fs: afero.NewOsFs()}, filepath.Join(t.TempDir(), "lock"))
}

// Two instances starting at the same moment on a fresh data directory both used to create the
// lock file and both come away holding it: O_CREATE without O_EXCL is not a claim, it is an
// open. This is the one race here that a filesystem settles outright.
func TestOnlyOneInstanceCanClaimAFreshLock(t *testing.T) {
	onRealFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		for attempt := 0; attempt < 30; attempt++ {
			if err := afs.RemoveAll(lockFile); err != nil {
				t.Fatal(err)
			}
			if won := countWinners(t, afs, lockFile, time.Minute, 8); won != 1 {
				t.Fatalf("%d of 8 instances claimed a fresh lock on attempt %d, want exactly 1", won, attempt)
			}
		}
	})
}

// And two instances that both find the same lease expired must not both take it over. There is
// no compare-and-swap on a filesystem, so a takeover is confirmed by reading the file back
// after a pause rather than assumed, and whoever is not in it backs off.
func TestOnlyOneInstanceCanTakeOverAnExpiredLease(t *testing.T) {
	onRealFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		const lease = 100 * time.Millisecond
		for attempt := 0; attempt < 30; attempt++ {
			// a lock file whose holder went quiet long ago
			if err := afs.WriteFile(lockFile, []byte("departed-instance"), 0660); err != nil {
				t.Fatal(err)
			}
			age(t, afs, lockFile, 10*lease)

			if won := countWinners(t, afs, lockFile, lease, 8); won != 1 {
				t.Fatalf("%d of 8 instances took over an expired lease on attempt %d, want exactly 1", won, attempt)
			}
		}
	})
}

// A claim is never half-written, however many instances are writing one. The truncate-then-
// write this replaced let a reader see an empty file or a short read, which a holder renewing
// its own lease would take as having lost it.
func TestAClaimIsNeverReadHalfWritten(t *testing.T) {
	onRealFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		writers := make([]FileLock, 4)
		ids := map[string]bool{}
		for i := range writers {
			writers[i] = NewFileLock(afs, lockFile, time.Minute, time.Hour, failOnLoss(t))
			ids[writers[i].uniqueId] = true
		}
		if err := afs.WriteFile(lockFile, []byte(writers[0].uniqueId), 0660); err != nil {
			t.Fatal(err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func(lock FileLock) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := lock.writeClaim(); err != nil {
						t.Error(err)
						return
					}
				}
			}(writers[i])
		}
		for i := 0; i < 2000; i++ {
			owner, err := writers[0].readOwner()
			if err != nil {
				close(stop)
				wg.Wait()
				t.Fatalf("read the lock file and got %v, want a whole id", err)
			}
			if !ids[owner] {
				close(stop)
				wg.Wait()
				t.Fatalf("read [%s] from the lock file, which is not a whole id any writer wrote", owner)
			}
		}
		close(stop)
		wg.Wait()
	})
}

// A live lease is nobody else's to take, however many ask.
func TestNobodyTakesALiveLease(t *testing.T) {
	forEachFs(t, func(t *testing.T, afs *afero.Afero, lockFile string) {
		holder := NewFileLock(afs, lockFile, time.Minute, time.Hour, failOnLoss(t))
		if !holder.AcquireLock() {
			t.Fatal("unable to acquire a free lock")
		}

		if won := countWinners(t, afs, lockFile, time.Minute, 8); won != 0 {
			t.Errorf("%d instances took a lease that was still live", won)
		}
		if owner := ownerOf(t, afs, lockFile); owner != holder.uniqueId {
			t.Errorf("the lock file is held by [%s], want the holder [%s]", owner, holder.uniqueId)
		}
	})
}

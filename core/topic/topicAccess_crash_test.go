package topic

import (
	"math/rand"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
)

// Crash recovery of the core, with the fault injected below the adapter: the process dies
// with a write half on the media, and a restarted server has to make sense of what is left.

// newCrashStore returns a store on a real directory whose media can be torn.
func newCrashStore(t *testing.T) (*filestore.Store, *faultfs.CrashFiles) {
	t.Helper()
	fs := faultfs.NewCrashFiles(filestore.OS{})
	return filestore.New(fs, t.TempDir()), fs
}

// forEachCrashMedia used to run each crash twice, the second time against an emulated
// filesystem. A torn write is only worth testing where it tears the way real media tears,
// and the emulation disagreed with real media twice over open flags alone.
func forEachCrashMedia(t *testing.T, run func(t *testing.T, store *filestore.Store, fs *faultfs.CrashFiles)) {
	t.Helper()
	store, fs := newCrashStore(t)
	run(t, store, fs)
}

// TestTopic_CrashDuringWriteKeepsAcknowledgedEntries is the promise the log makes: a write
// that was acknowledged is still there after the crash, and the torn entry of the write
// that was not acknowledged is gone.
func TestTopic_CrashDuringWriteKeepsAcknowledgedEntries(t *testing.T) {
	forEachCrashMedia(t, func(t *testing.T, store *filestore.Store, fs *faultfs.CrashFiles) {
		topic := newTestTopic(t, store, 2000)
		acknowledged := writeRandomBatches(t, topic, rand.New(rand.NewSource(11)), 200)
		topic.indexWg.Wait()

		// the crash lands in the middle of the next batch, which is never acknowledged
		fs.ArmAfterFor(".log", 37)
		batch := payloads(acknowledged, 20)
		if err := topic.Write(&batch); err == nil {
			t.Fatal("the write was acknowledged although the process crashed")
		}
		fs.Restart()

		recovered := newTestTopic(t, store, 2000)
		if int(recovered.NextOffset) < acknowledged {
			t.Fatalf("NextOffset=%d after recovery, want at least the %d acknowledged entries",
				recovered.NextOffset, acknowledged)
		}
		if int(recovered.NextOffset) >= acknowledged+20 {
			t.Fatalf("NextOffset=%d after recovery, want less than the %d entries of the torn batch",
				recovered.NextOffset, acknowledged+20)
		}
		assertReadsFromEveryOffset(t, recovered, int(recovered.NextOffset))

		// the recovered log takes writes again and stays readable end to end
		n := int(recovered.NextOffset)
		writeEntries(t, recovered, n, 25)
		n += 25
		if _, err := recovered.UpdateIndex(); err != nil {
			t.Fatal(err)
		}
		assertIndexMatchesFullScan(t, recovered)
		assertReadsFromEveryOffset(t, recovered, n)
		assertReadsFromEveryOffset(t, newTestTopic(t, store, 2000), n)
	})
}

// TestTopic_CrashDuringIndexWriteDropsTheTornPair tears an index block in the middle of an
// (offset, byteOffset) pair, which is the one place where half a write is not half an entry.
func TestTopic_CrashDuringIndexWriteDropsTheTornPair(t *testing.T) {
	forEachCrashMedia(t, func(t *testing.T, store *filestore.Store, fs *faultfs.CrashFiles) {
		topic := newTestTopic(t, store, 1<<20)
		n := writeRandomBatches(t, topic, rand.New(rand.NewSource(13)), 200)
		topic.indexWg.Wait()
		if _, err := topic.UpdateIndex(); err != nil {
			t.Fatal(err)
		}

		// Leave the index lagging the log, the way it lags after any write that has not been
		// indexed yet, by dropping its last pairs and reloading so the topic resumes from
		// what is on the media. Doing it this way rather than by writing while the crash is
		// armed is what makes this test deterministic: a write waits on a flush of the log
		// block, and a crash tripped by the background indexer fails that flush too, so the
		// write would fail for reasons the test is not about. That was a one-in-thirty flake.
		indexRef := topic.indexRef(domain.IndexBlock(topic.LogBlockList[0]))
		lagging := int64(len(blockBytes(t, store, indexRef))) - 5*indexPairSize
		if lagging < 0 {
			t.Fatalf("setup: the index holds fewer than five pairs")
		}
		if err := store.Truncate(indexRef, lagging); err != nil {
			t.Fatal(err)
		}
		topic = newTestTopic(t, store, 1<<20)

		// half of one pair reaches the media, and the crash takes the rollback with it
		fs.ArmAfterFor(".idx", indexPairSize/2)
		_, _ = topic.UpdateIndex()
		fs.Restart()

		indexBytes := blockBytes(t, store, indexRef)
		if len(indexBytes)%indexPairSize == 0 {
			t.Skip("the crash did not tear a pair on this filesystem")
		}

		recovered := newTestTopic(t, store, 1<<20)
		if int(recovered.NextOffset) != n {
			t.Fatalf("NextOffset=%d after recovery, want %d: the log was not touched", recovered.NextOffset, n)
		}
		if _, err := recovered.UpdateIndex(); err != nil {
			t.Fatal(err)
		}
		assertIndexMatchesFullScan(t, recovered)
		assertReadsFromEveryOffset(t, recovered, n)
	})
}

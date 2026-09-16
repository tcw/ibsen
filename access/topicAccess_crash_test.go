package access

import (
	"math/rand"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/blockstore/aferostore"
	"github.com/tcw/ibsen/access/blockstore/faultfs"
	"github.com/tcw/ibsen/access/common"
)

// Crash recovery of the core, with the fault injected below the adapter: the process dies
// with a write half on the media, and a restarted server has to make sense of what is left.

// newCrashStore returns a store whose media can be torn, on both an in-memory filesystem
// and a real directory.
func newCrashStore(t *testing.T, fsName string) (*aferostore.Store, *faultfs.CrashFs) {
	t.Helper()
	if fsName == "os" {
		fs := faultfs.NewCrash(afero.NewOsFs())
		return aferostore.New(&afero.Afero{Fs: fs}, t.TempDir()), fs
	}
	fs := faultfs.NewCrash(afero.NewMemMapFs())
	afs := &afero.Afero{Fs: fs}
	if err := afs.MkdirAll("tmp", 0744); err != nil {
		t.Fatal(err)
	}
	return aferostore.New(afs, "tmp"), fs
}

func forEachCrashMedia(t *testing.T, run func(t *testing.T, store *aferostore.Store, fs *faultfs.CrashFs)) {
	t.Helper()
	for _, fsName := range []string{"mem", "os"} {
		t.Run(fsName, func(t *testing.T) {
			store, fs := newCrashStore(t, fsName)
			run(t, store, fs)
		})
	}
}

// TestTopic_CrashDuringWriteKeepsAcknowledgedEntries is the promise the log makes: a write
// that was acknowledged is still there after the crash, and the torn entry of the write
// that was not acknowledged is gone.
func TestTopic_CrashDuringWriteKeepsAcknowledgedEntries(t *testing.T) {
	forEachCrashMedia(t, func(t *testing.T, store *aferostore.Store, fs *faultfs.CrashFs) {
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
	forEachCrashMedia(t, func(t *testing.T, store *aferostore.Store, fs *faultfs.CrashFs) {
		topic := newTestTopic(t, store, 1<<20)
		n := writeRandomBatches(t, topic, rand.New(rand.NewSource(13)), 200)
		topic.indexWg.Wait()
		if _, err := topic.UpdateIndex(); err != nil {
			t.Fatal(err)
		}

		// half of one pair reaches the media, and the crash takes the rollback with it
		fs.ArmAfterFor(".idx", indexPairSize/2)
		writeEntries(t, topic, n, 40)
		n += 40
		_, _ = topic.UpdateIndex()
		fs.Restart()

		indexBytes := blockBytes(t, store, topic.indexRef(common.IndexBlock(topic.LogBlockList[0])))
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

package aferostore

import (
	"errors"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/core/port/driven"
)

// crashStores gives the same crash tests a filesystem in memory and one on disk, since a
// torn write is only interesting if it tears the same way on real media.
func crashStores(t *testing.T) map[string]func(t *testing.T) (*Store, *faultfs.CrashFs) {
	t.Helper()
	return map[string]func(t *testing.T) (*Store, *faultfs.CrashFs){
		"mem": func(t *testing.T) (*Store, *faultfs.CrashFs) {
			fs := faultfs.NewCrash(afero.NewMemMapFs())
			afs := &afero.Afero{Fs: fs}
			if err := afs.MkdirAll("data", 0744); err != nil {
				t.Fatal(err)
			}
			return New(afs, "data"), fs
		},
		"os": func(t *testing.T) (*Store, *faultfs.CrashFs) {
			fs := faultfs.NewCrash(afero.NewOsFs())
			return New(&afero.Afero{Fs: fs}, t.TempDir()), fs
		},
	}
}

func forEachCrashStore(t *testing.T, run func(t *testing.T, store *Store, fs *faultfs.CrashFs)) {
	t.Helper()
	for name, newStore := range crashStores(t) {
		t.Run(name, func(t *testing.T) {
			store, fs := newStore(t)
			run(t, store, fs)
		})
	}
}

// TestCrash_LeavesAPrefixOfTheAppend is the property every bit of recovery rests on: a
// crash can cut an append short, but it never rewrites what was already in the block.
func TestCrash_LeavesAPrefixOfTheAppend(t *testing.T) {
	forEachCrashStore(t, func(t *testing.T, store *Store, fs *faultfs.CrashFs) {
		ref := driven.LogRef("topic", 0)
		if _, err := store.Append(ref, []byte("acknowledged.")); err != nil {
			t.Fatal(err)
		}
		fs.ArmAfter(5)
		if _, err := store.Append(ref, []byte("torn in the middle")); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("append err=%v, want the crash", err)
		}
		if !fs.Crashed() {
			t.Fatal("the filesystem did not crash")
		}
		fs.Restart()

		content := readAll(t, store, ref, 0)
		if content != "acknowledged.torn " {
			t.Fatalf("after the crash the block holds %q", content)
		}
	})
}

// TestCrash_DuringAppendReportsDirtyBlock: a crash takes the rollback with it, so the
// caller has to be told the block may end in a partial write.
func TestCrash_DuringAppendReportsDirtyBlock(t *testing.T) {
	forEachCrashStore(t, func(t *testing.T, store *Store, fs *faultfs.CrashFs) {
		ref := driven.LogRef("topic", 0)
		if _, err := store.Append(ref, []byte("acknowledged.")); err != nil {
			t.Fatal(err)
		}
		fs.ArmAfter(3)
		_, err := store.Append(ref, []byte("torn"))
		if !errors.Is(err, driven.ErrDirtyBlock) {
			t.Fatalf("append err=%v, want ErrDirtyBlock", err)
		}
		if !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("append err=%v, want it to keep the cause", err)
		}
	})
}

// TestCrash_BeforeAnyByteLandsLeavesTheBlockUntouched: an append that never reached the
// media is not a torn write, and recovery must find the block exactly as it was.
func TestCrash_BeforeAnyByteLandsLeavesTheBlockUntouched(t *testing.T) {
	forEachCrashStore(t, func(t *testing.T, store *Store, fs *faultfs.CrashFs) {
		ref := driven.LogRef("topic", 0)
		if _, err := store.Append(ref, []byte("acknowledged.")); err != nil {
			t.Fatal(err)
		}
		fs.ArmAfter(0)
		if _, err := store.Append(ref, []byte("never lands")); err == nil {
			t.Fatal("the append survived the crash")
		}
		fs.Restart()
		if content := readAll(t, store, ref, 0); content != "acknowledged." {
			t.Fatalf("after the crash the block holds %q", content)
		}
		if size := blockSizeOf(t, store, ref); size != int64(len("acknowledged.")) {
			t.Fatalf("the store reports %d bytes", size)
		}
	})
}

// TestCrash_AfterTheWholeAppendKeepsIt is the other end of the same case: the bytes reached
// the media before the process died, so they are there after the restart even though the
// caller was told the append failed.
func TestCrash_AfterTheWholeAppendKeepsIt(t *testing.T) {
	forEachCrashStore(t, func(t *testing.T, store *Store, fs *faultfs.CrashFs) {
		ref := driven.LogRef("topic", 0)
		fs.ArmAfter(len("landed"))
		if _, err := store.Append(ref, []byte("landed")); err == nil {
			t.Fatal("the append reported success although the process crashed")
		}
		fs.Restart()
		if content := readAll(t, store, ref, 0); content != "landed" {
			t.Fatalf("after the crash the block holds %q", content)
		}
	})
}

// TestCrash_DuringTruncateLeavesTheLongerBlock: recovery that cannot finish must leave the
// torn tail rather than a half cut block, so the next attempt sees the same thing.
func TestCrash_DuringTruncateLeavesTheLongerBlock(t *testing.T) {
	forEachCrashStore(t, func(t *testing.T, store *Store, fs *faultfs.CrashFs) {
		ref := driven.LogRef("topic", 0)
		if _, err := store.Append(ref, []byte("kept and torn")); err != nil {
			t.Fatal(err)
		}
		fs.ArmAfter(0)
		if _, err := store.Append(ref, []byte("x")); err == nil {
			t.Fatal("the append survived the crash")
		}
		if err := store.Truncate(ref, 4); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("truncate during a crash: err=%v, want the crash", err)
		}
		fs.Restart()
		if content := readAll(t, store, ref, 0); content != "kept and torn" {
			t.Fatalf("after the crash the block holds %q", content)
		}
	})
}

func blockSizeOf(t *testing.T, store *Store, ref driven.BlockRef) int64 {
	t.Helper()
	blocks, err := store.List(ref.Topic, ref.Kind)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range blocks {
		if block.Block == ref.Block {
			return block.Size
		}
	}
	t.Fatalf("%s is not among %v", ref, blocks)
	return 0
}

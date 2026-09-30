package filestore_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/port/driven"
)

// A synced block is durable only if every name on the path to it is. Sync makes the block's
// own name durable by syncing the topic directory when the block is new, but the topic
// directory's name lives in the root, and nothing syncs the root: a power cut after the first
// write to a new topic can take the whole topic, acknowledged entries and all. Found by the
// nemesis test in adapter/driver/history.
func TestPowerCut_aSyncedBlockInANewTopicSurvives(t *testing.T) {
	dir := t.TempDir()
	cache := faultfs.NewPageCache()
	store := filestore.New(cache, dir)
	ref := driven.BlockRef{Topic: "new", Kind: driven.Log, Block: 0}
	if _, err := store.CreateTopic("new"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ref, []byte("acknowledged")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err != nil {
		t.Fatal(err)
	}

	cache.Crash()
	if err := cache.Recover(faultfs.LoseEverything()); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, filestore.NewOS(dir), ref, 0); got != "acknowledged" {
		t.Fatalf("a synced block holds %q after a power cut", got)
	}
}

// crashAtDirSync cuts the power the moment a directory is opened to be synced: after the
// block's own fsync, before the fsync that makes its name durable.
type crashAtDirSync struct {
	*faultfs.PageCache
}

func (c crashAtDirSync) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	if info, err := os.Stat(name); err == nil && info.IsDir() {
		c.Crash()
	}
	return c.PageCache.OpenFile(name, flag, perm)
}

// The block's bytes are on the media and its name is not, so the block is gone after the power
// cut, and Sync must not have said otherwise. It used to swallow the failed directory sync and
// report the block durable, which acknowledged a write the power cut then took. Found by the
// nemesis test in adapter/driver/history.
func TestPowerCut_aSyncCutBeforeTheDirectoryIsNotDurable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "topic"), 0744); err != nil {
		t.Fatal(err)
	}
	cache := faultfs.NewPageCache()
	store := filestore.New(crashAtDirSync{cache}, dir)
	ref := driven.BlockRef{Topic: "topic", Kind: driven.Log, Block: 0}
	if _, err := store.Append(ref, []byte("unacknowledged")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err == nil {
		t.Fatal("Sync reported a block durable whose name was not")
	}

	if err := cache.Recover(faultfs.LoseEverything()); err != nil {
		t.Fatal(err)
	}
	if _, err := filestore.NewOS(dir).Open(ref, 0); !errors.Is(err, driven.ErrBlockNotFound) {
		t.Fatalf("the model kept the block after all (%v), so this test proves nothing", err)
	}
}

// A directory sync that failed is not forgotten by a restart. The store used to remember
// which names it had created and not yet made durable, and a new process remembers nothing,
// so it took the block's name as durable, never synced the directory again, and acknowledged
// writes into a block a power cut then took, all of it. Found by the crash at every storage
// call in adapter/driver/history.
func TestPowerCut_aRestartRetriesTheDirectorySyncThatFailed(t *testing.T) {
	for _, tc := range []struct {
		name string
		// which sync of the first store's first Sync fails: the block's, its directory's,
		// then the root's
		failing int
	}{
		{"the topic directory", 2},
		{"the root", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cache := faultfs.NewPageCache()
			ref := driven.BlockRef{Topic: "topic", Kind: driven.Log, Block: 0}
			first := filestore.New(cache, dir)
			if _, err := first.CreateTopic("topic"); err != nil {
				t.Fatal(err)
			}
			if _, err := first.Append(ref, []byte("before,")); err != nil {
				t.Fatal(err)
			}
			cache.FailAtCall(len(cache.Trace()) + tc.failing)
			if err := first.Sync(ref); err == nil {
				t.Fatal("the sync did not fail")
			}

			// a new process, which has only the directory to go on
			second := filestore.New(cache, dir)
			if _, err := second.Append(ref, []byte("acknowledged")); err != nil {
				t.Fatal(err)
			}
			if err := second.Sync(ref); err != nil {
				t.Fatal(err)
			}
			cache.Crash()
			if err := cache.Recover(faultfs.LoseEverything()); err != nil {
				t.Fatal(err)
			}
			if got := readAll(t, filestore.NewOS(dir), ref, 0); got != "before,acknowledged" {
				t.Fatalf("after the power cut the block holds %q", got)
			}
		})
	}
}

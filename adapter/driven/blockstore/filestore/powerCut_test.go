package filestore_test

import (
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

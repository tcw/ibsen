package filestore_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/port/driven"
)

// newDiskStore returns a store on a real directory, which is the only kind this adapter has.
func newDiskStore(t *testing.T) (*filestore.Store, string) {
	t.Helper()
	root := t.TempDir()
	return filestore.NewOS(root), root
}

// blockFileOnDisk is the path the adapter keeps a block at, which a durability test reads
// without going through the port.
func blockFileOnDisk(root string, ref driven.BlockRef) string {
	return filepath.Join(root, string(ref.Topic), filestore.BlockFileName(ref))
}

func TestDurability_SyncedBytesAreOnDisk(t *testing.T) {
	store, root := newDiskStore(t)
	ref := driven.LogRef("topic", 0)
	if _, err := store.Append(ref, []byte("durable")); err != nil {
		t.Fatal(err)
	}
	synced, err := driven.Sync(store, ref)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !synced {
		t.Fatal("the filesystem adapter must be Syncable")
	}
	content, err := os.ReadFile(blockFileOnDisk(root, ref))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "durable" {
		t.Fatalf("the file on disk holds %q", content)
	}
}

// TestDurability_SyncAfterEveryAppend is the shape a flush policy would take: the caller
// decides when to sync, and once Sync returns the bytes are the store's own answer too.
func TestDurability_SyncAfterEveryAppend(t *testing.T) {
	store, root := newDiskStore(t)
	ref := driven.LogRef("topic", 7)
	var want string
	for _, chunk := range []string{"one", "two", "three"} {
		block, err := store.Append(ref, []byte(chunk))
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Sync(ref); err != nil {
			t.Fatalf("sync: %v", err)
		}
		want += chunk
		content, err := os.ReadFile(blockFileOnDisk(root, ref))
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != want {
			t.Fatalf("after syncing %q the file holds %q", chunk, content)
		}
		if block.Size != int64(len(want)) {
			t.Fatalf("append reported %d bytes, the file holds %d", block.Size, len(want))
		}
	}
}

func TestDurability_SyncOfUnknownBlock(t *testing.T) {
	store, _ := newDiskStore(t)
	if err := store.Sync(driven.LogRef("topic", 0)); !errors.Is(err, driven.ErrBlockNotFound) {
		t.Fatalf("sync of an unknown block: err=%v, want ErrBlockNotFound", err)
	}
}

// TestDurability_TruncatedBlockIsSyncable covers recovery: the block a crash cut back to a
// valid size has to be made durable at that size, or the next crash finds the torn tail again.
func TestDurability_TruncatedBlockIsSyncable(t *testing.T) {
	store, root := newDiskStore(t)
	ref := driven.LogRef("topic", 0)
	if _, err := store.Append(ref, []byte("kept and torn")); err != nil {
		t.Fatal(err)
	}
	if err := store.Truncate(ref, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(blockFileOnDisk(root, ref))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "kept" {
		t.Fatalf("the file on disk holds %q", content)
	}
}

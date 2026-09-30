package filestore_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/port/driven"
)

// droppingFS records which files the store asked the kernel to drop from its cache.
type droppingFS struct {
	filestore.FS
	mu      sync.Mutex
	dropped []string
}

func (d *droppingFS) DropCache(name string) error {
	d.mu.Lock()
	d.dropped = append(d.dropped, filepath.Base(name))
	d.mu.Unlock()
	return d.FS.DropCache(name)
}

// The head block, and only it, is dropped from the cache the first time a store lists a
// topic, which is before the core recovers it: a failed fsync can only have left its hole
// there, and only a store's first recovery can meet one a process before it left.
func TestList_dropsTheHeadBlockFromTheCacheOncePerTopic(t *testing.T) {
	root := t.TempDir()
	for _, block := range []uint64{0, 100, 200} {
		if _, err := filestore.NewOS(root).Append(driven.BlockRef{Topic: "topic", Kind: driven.Log, Block: block}, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	fs := &droppingFS{FS: filestore.OS{}}
	store := filestore.New(fs, root)
	for i := 0; i < 3; i++ {
		if _, err := store.List("topic", driven.Log); err != nil {
			t.Fatal(err)
		}
	}
	if len(fs.dropped) != 1 || fs.dropped[0] != "00000000000000000200.log" {
		t.Fatalf("dropped %v, want the head block once", fs.dropped)
	}
}

// A reader recovers nothing, so a read-only store never drops the cache: every `ibsen cat`
// would otherwise read the log cold.
func TestList_aReadOnlyStoreKeepsTheCache(t *testing.T) {
	root := t.TempDir()
	if _, err := filestore.NewOS(root).Append(driven.LogRef("topic", 0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	fs := &droppingFS{FS: filestore.OS{}}
	if _, err := filestore.New(filestore.ReadOnly{FS: fs}, root).List("topic", driven.Log); err != nil {
		t.Fatal(err)
	}
	if len(fs.dropped) != 0 {
		t.Fatalf("a read-only store dropped %v", fs.dropped)
	}
}

// The real call works on this machine's filesystem, whatever it does or does not drop.
func TestOS_dropCacheOfARealFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := (filestore.OS{}).DropCache(path); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != "data" {
		t.Fatalf("the file reads %q after dropping its cache", got)
	}
}

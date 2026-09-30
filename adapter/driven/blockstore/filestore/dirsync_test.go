package filestore_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// These tests are about how many fsyncs a sync costs, which is not something the port can
// see: they count them through the store's own seam. They live outside the package for the
// same reason the crash tests do — nothing here reaches anything unexported.

// countingFS counts the syncs made through it, by path. It is the real filesystem underneath:
// what is counted is what a disk was actually asked to do.
type countingFS struct {
	filestore.FS
	mu    sync.Mutex
	syncs map[string]int
}

func newCountingFS() *countingFS {
	return &countingFS{FS: filestore.OS{}, syncs: make(map[string]int)}
}

func (c *countingFS) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	file, err := c.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: file, name: name, fs: c}, nil
}

func (c *countingFS) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncs[name]
}

type countingFile struct {
	filestore.File
	name string
	fs   *countingFS
}

func (f *countingFile) Sync() error {
	f.fs.mu.Lock()
	f.fs.syncs[f.name]++
	f.fs.mu.Unlock()
	return f.File.Sync()
}

// TestSync_SyncsTheDirectoryOnlyForANewBlock is what a log pays per acknowledged write. The
// block's own fsync is the one that makes the bytes durable and happens every time; the
// directory's makes the block's name durable and is only needed once, when the file appears.
func TestSync_SyncsTheDirectoryOnlyForANewBlock(t *testing.T) {
	fs := newCountingFS()
	root := t.TempDir()
	store := filestore.New(fs, root)
	topicDir := filepath.Join(root, "topic")
	ref := driven.LogRef("topic", 0)
	blockFile := blockFileOnDisk(root, ref)

	if _, err := store.Append(ref, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err != nil {
		t.Fatal(err)
	}
	if got := fs.count(blockFile); got != 1 {
		t.Fatalf("the new block was synced %d times, want 1", got)
	}
	if got := fs.count(topicDir); got != 1 {
		t.Fatalf("the directory of a new block was synced %d times, want 1", got)
	}

	for i := 0; i < 3; i++ {
		if _, err := store.Append(ref, []byte("more")); err != nil {
			t.Fatal(err)
		}
		if err := store.Sync(ref); err != nil {
			t.Fatal(err)
		}
	}
	if got := fs.count(blockFile); got != 4 {
		t.Fatalf("the block was synced %d times, want one per append", got)
	}
	if got := fs.count(topicDir); got != 1 {
		t.Fatalf("appending to a named block synced its directory %d times, want 1", got)
	}
}

// TestSync_SyncsTheDirectoryAgainForTheNextBlock covers the block roll: a new file means a
// new directory entry, whatever was synced for the block before it.
func TestSync_SyncsTheDirectoryAgainForTheNextBlock(t *testing.T) {
	fs := newCountingFS()
	root := t.TempDir()
	store := filestore.New(fs, root)
	topicDir := filepath.Join(root, "topic")

	for _, block := range []domain.LogBlock{0, 100} {
		ref := driven.LogRef("topic", block)
		if _, err := store.Append(ref, []byte("entries")); err != nil {
			t.Fatal(err)
		}
		if err := store.Sync(ref); err != nil {
			t.Fatal(err)
		}
	}
	if got := fs.count(topicDir); got != 2 {
		t.Fatalf("two new blocks synced the directory %d times, want 2", got)
	}

	// the index block is a third file in the same directory, and earns its own sync
	index := driven.IndexRef("topic", 0)
	if _, err := store.Append(index, []byte("pairs")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(index); err != nil {
		t.Fatal(err)
	}
	if got := fs.count(topicDir); got != 3 {
		t.Fatalf("a new index block synced the directory %d times, want 3", got)
	}
}

// TestSync_RetriesTheDirectoryWhenItFails pins that a failed directory sync is not forgotten:
// the block stays new, so the next sync of it tries again. The bytes are durable either way,
// which is why the failure is not returned.
func TestSync_RetriesTheDirectoryWhenItFails(t *testing.T) {
	fs := &failingDirFS{countingFS: newCountingFS(), failDirSync: true}
	root := t.TempDir()
	store := filestore.New(fs, root)
	topicDir := filepath.Join(root, "topic")
	ref := driven.LogRef("topic", 0)

	if _, err := store.Append(ref, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err != nil {
		t.Fatalf("a failed directory sync must not fail the block's: %v", err)
	}
	if got := fs.count(topicDir); got != 1 {
		t.Fatalf("the directory was synced %d times, want 1", got)
	}

	fs.failDirSync = false
	if _, err := store.Append(ref, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(ref); err != nil {
		t.Fatal(err)
	}
	if got := fs.count(topicDir); got != 2 {
		t.Fatalf("the directory was synced %d times, want the failed one to be retried", got)
	}
}

// failingDirFS fails the sync of a directory, which is the one error Sync swallows.
type failingDirFS struct {
	*countingFS
	failDirSync bool
}

func (f *failingDirFS) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	file, err := f.countingFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return &failingDirFile{File: file, fs: f}, nil
	}
	return file, nil
}

type failingDirFile struct {
	filestore.File
	fs *failingDirFS
}

func (f *failingDirFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	if f.fs.failDirSync {
		return os.ErrInvalid
	}
	return nil
}

// TestSync_SyncsTheRootOnlyForANewTopic is the same rule one level up: a topic this store
// created has a name in the root that no fsync has made durable, so its first sync pays for
// the root once. A topic that was already there when the store opened, and every later block
// of a new one, pays nothing.
func TestSync_SyncsTheRootOnlyForANewTopic(t *testing.T) {
	fs := newCountingFS()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "existing"), 0744); err != nil {
		t.Fatal(err)
	}
	store := filestore.New(fs, root)
	if _, err := store.CreateTopic("created"); err != nil {
		t.Fatal(err)
	}

	for _, ref := range []driven.BlockRef{
		driven.LogRef("created", 0), driven.LogRef("created", 100), driven.LogRef("existing", 0),
		driven.LogRef("appended", 0),
	} {
		if _, err := store.Append(ref, []byte("entries")); err != nil {
			t.Fatal(err)
		}
		if err := store.Sync(ref); err != nil {
			t.Fatal(err)
		}
	}
	// one for the topic CreateTopic made, one for the topic the first append made
	if got := fs.count(root); got != 2 {
		t.Fatalf("the root was synced %d times, want once for each new topic", got)
	}
}

package aferostore

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
)

func newStore(t *testing.T) (*Store, *afero.Afero) {
	t.Helper()
	store, afs := NewMem("data")
	return store, afs
}

func mustAppend(t *testing.T, store *Store, ref common.BlockRef, data string) common.Block {
	t.Helper()
	block, err := store.Append(ref, []byte(data))
	if err != nil {
		t.Fatalf("append %s: %v", ref, err)
	}
	return block
}

func readAll(t *testing.T, store *Store, ref common.BlockRef, byteOffset int64) string {
	t.Helper()
	reader, err := store.Open(ref, byteOffset)
	if err != nil {
		t.Fatalf("open %s: %v", ref, err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read %s: %v", ref, err)
	}
	return string(content)
}

func TestStore_AppendAndOpen(t *testing.T) {
	store, _ := newStore(t)
	ref := common.LogRef("topic", 0)
	if block := mustAppend(t, store, ref, "hello "); block.Size != 6 {
		t.Fatalf("size after first append is %d, want 6", block.Size)
	}
	if block := mustAppend(t, store, ref, "world"); block.Size != 11 {
		t.Fatalf("size after second append is %d, want 11", block.Size)
	}
	if got := readAll(t, store, ref, 0); got != "hello world" {
		t.Fatalf("read %q", got)
	}
	if got := readAll(t, store, ref, 6); got != "world" {
		t.Fatalf("read from byte 6: %q", got)
	}
	if got := readAll(t, store, ref, 11); got != "" {
		t.Fatalf("read from the end: %q", got)
	}
}

func TestStore_AppendCreatesTopic(t *testing.T) {
	store, afs := newStore(t)
	mustAppend(t, store, common.LogRef("made-by-append", 0), "x")
	exists, err := afs.DirExists("data/made-by-append")
	if err != nil || !exists {
		t.Fatalf("topic directory exists=%v, err=%v", exists, err)
	}
	topics, err := store.Topics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0] != "made-by-append" {
		t.Fatalf("topics=%v", topics)
	}
}

func TestStore_CreateTopicReportsCreation(t *testing.T) {
	store, _ := newStore(t)
	created, err := store.CreateTopic("topic")
	if err != nil || !created {
		t.Fatalf("first create: created=%v, err=%v", created, err)
	}
	created, err = store.CreateTopic("topic")
	if err != nil || created {
		t.Fatalf("second create: created=%v, err=%v", created, err)
	}
}

func TestStore_ListOrdersBlocksAndSeparatesKinds(t *testing.T) {
	store, afs := newStore(t)
	for _, block := range []common.LogBlock{42, 0, 7} {
		mustAppend(t, store, common.LogRef("topic", block), "log")
	}
	mustAppend(t, store, common.IndexRef("topic", 7), "idxidx")
	if err := afs.WriteFile("data/topic/notes.txt", []byte("stray"), 0600); err != nil {
		t.Fatal(err)
	}

	logs, err := store.List("topic", common.Log)
	if err != nil {
		t.Fatal(err)
	}
	want := []common.Block{{Block: 0, Size: 3}, {Block: 7, Size: 3}, {Block: 42, Size: 3}}
	if len(logs) != len(want) {
		t.Fatalf("log blocks=%v, want %v", logs, want)
	}
	for i := range want {
		if logs[i] != want[i] {
			t.Fatalf("log blocks=%v, want %v", logs, want)
		}
	}
	indexes, err := store.List("topic", common.Index)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0] != (common.Block{Block: 7, Size: 6}) {
		t.Fatalf("index blocks=%v", indexes)
	}
	stray, err := store.StrayFiles("topic")
	if err != nil || len(stray) != 1 || stray[0] != "notes.txt" {
		t.Fatalf("stray=%v, err=%v", stray, err)
	}
}

func TestStore_UnknownTopicAndBlock(t *testing.T) {
	store, _ := newStore(t)
	blocks, err := store.List("nope", common.Log)
	if err != nil || blocks != nil {
		t.Fatalf("list of an unknown topic: %v, %v", blocks, err)
	}
	if _, err := store.Open(common.LogRef("nope", 0), 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("open err=%v, want ErrBlockNotFound", err)
	}
	if err := store.Truncate(common.LogRef("nope", 0), 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("truncate err=%v, want ErrBlockNotFound", err)
	}
	if err := store.Remove(common.LogRef("nope", 0)); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("remove err=%v, want ErrBlockNotFound", err)
	}
}

func TestStore_TruncateAndRemove(t *testing.T) {
	store, _ := newStore(t)
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, "hello world")
	if err := store.Truncate(ref, 12); !errors.Is(err, common.ErrInvalidSize) {
		t.Fatalf("truncate past the end: err=%v, want ErrInvalidSize", err)
	}
	if err := store.Truncate(ref, 5); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, store, ref, 0); got != "hello" {
		t.Fatalf("after truncate: %q", got)
	}
	mustAppend(t, store, ref, "!")
	if got := readAll(t, store, ref, 0); got != "hello!" {
		t.Fatalf("append after truncate: %q", got)
	}
	if err := store.Remove(ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(ref, 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("open after remove: err=%v", err)
	}
}

// failingFs fails every write once armed, after passing part of it on, like a full disk.
type failingFs struct {
	afero.Fs
	failWrites    atomic.Bool
	failTruncates atomic.Bool
}

type failingFile struct {
	afero.File
	fs *failingFs
}

var errInjected = errors.New("injected failure")

func (f *failingFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &failingFile{File: file, fs: f}, nil
}

func (f *failingFile) Write(p []byte) (int, error) {
	if f.fs.failWrites.Load() {
		n, _ := f.File.Write(p[:len(p)/2])
		return n, errInjected
	}
	return f.File.Write(p)
}

func (f *failingFile) Truncate(size int64) error {
	if f.fs.failTruncates.Load() {
		return errInjected
	}
	return f.File.Truncate(size)
}

func TestStore_FailedAppendIsRolledBack(t *testing.T) {
	fs := &failingFs{Fs: afero.NewMemMapFs()}
	afs := &afero.Afero{Fs: fs}
	if err := afs.MkdirAll("data", 0744); err != nil {
		t.Fatal(err)
	}
	store := New(afs, "data")
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, "keep")

	fs.failWrites.Store(true)
	_, appendErr := store.Append(ref, []byte("lost bytes"))
	if !errors.Is(appendErr, errInjected) {
		t.Fatalf("append err=%v, want the injected failure", appendErr)
	}
	if errors.Is(appendErr, common.ErrDirtyBlock) {
		t.Fatal("a rolled back append must not report a dirty block")
	}
	fs.failWrites.Store(false)
	if got := readAll(t, store, ref, 0); got != "keep" {
		t.Fatalf("after a failed append: %q", got)
	}
	mustAppend(t, store, ref, "more")
	if got := readAll(t, store, ref, 0); got != "keepmore" {
		t.Fatalf("after appending again: %q", got)
	}
}

func TestStore_AppendReportsDirtyBlockWhenRollbackFails(t *testing.T) {
	fs := &failingFs{Fs: afero.NewMemMapFs()}
	afs := &afero.Afero{Fs: fs}
	if err := afs.MkdirAll("data", 0744); err != nil {
		t.Fatal(err)
	}
	store := New(afs, "data")
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, "keep")

	fs.failWrites.Store(true)
	fs.failTruncates.Store(true)
	_, appendErr := store.Append(ref, []byte("partially written"))
	if !errors.Is(appendErr, common.ErrDirtyBlock) {
		t.Fatalf("append err=%v, want ErrDirtyBlock", appendErr)
	}
	if !errors.Is(appendErr, errInjected) {
		t.Fatalf("append err=%v, want it to keep the cause", appendErr)
	}
}

func TestStore_SyncIsTheOptionalCapability(t *testing.T) {
	store, _ := newStore(t)
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, "durable")
	synced, err := common.Sync(store, ref)
	if err != nil || !synced {
		t.Fatalf("sync: synced=%v, err=%v", synced, err)
	}
	if err := store.Sync(common.LogRef("topic", 1)); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("sync of an unknown block: err=%v", err)
	}
}

package filestore

import (
	"errors"
	"io"
	"testing"

	"github.com/tcw/ibsen/core/port/driven"
)

// A read-only server refuses writes at the manager, but that is not the whole of what a
// server writes: loading a topic recovers its head block, which truncates a torn tail, and
// rebuilds its index. Pointed at a directory another instance owns, neither is ours to do.
func TestReadOnlyRefusesEverythingThatWouldChangeTheLog(t *testing.T) {
	root := t.TempDir()
	writable := NewOS(root)
	ref := driven.LogRef("topic", 0)
	if _, err := writable.Append(ref, []byte("already here")); err != nil {
		t.Fatal(err)
	}

	store := New(ReadOnly{FS: OS{}}, root)

	t.Run("append", func(t *testing.T) {
		if _, err := store.Append(ref, []byte("more")); !errors.Is(err, ErrReadOnly) {
			t.Errorf("append gave %v, want ErrReadOnly", err)
		}
	})
	t.Run("truncate", func(t *testing.T) {
		if err := store.Truncate(ref, 0); !errors.Is(err, ErrReadOnly) {
			t.Errorf("truncate gave %v, want ErrReadOnly", err)
		}
	})
	t.Run("remove", func(t *testing.T) {
		if err := store.Remove(ref); !errors.Is(err, ErrReadOnly) {
			t.Errorf("remove gave %v, want ErrReadOnly", err)
		}
	})
	t.Run("create topic", func(t *testing.T) {
		if _, err := store.CreateTopic("another"); !errors.Is(err, ErrReadOnly) {
			t.Errorf("creating a topic gave %v, want ErrReadOnly", err)
		}
	})
	t.Run("sync", func(t *testing.T) {
		if err := store.Sync(ref); !errors.Is(err, ErrReadOnly) {
			t.Errorf("sync gave %v, want ErrReadOnly", err)
		}
	})

	// and the log is exactly as it was
	blocks, err := store.List("topic", driven.Log)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].Size != int64(len("already here")) {
		t.Fatalf("the block is %v, want the twelve bytes that were there", blocks)
	}
}

// Reading is the point of a read-only store, so all of it still works.
func TestReadOnlyStillReads(t *testing.T) {
	root := t.TempDir()
	if _, err := NewOS(root).Append(driven.LogRef("topic", 0), []byte("readable")); err != nil {
		t.Fatal(err)
	}

	store := New(ReadOnly{FS: OS{}}, root)

	topics, err := store.Topics()
	if err != nil || len(topics) != 1 || topics[0] != "topic" {
		t.Fatalf("Topics gave %v, %v", topics, err)
	}
	reader, err := store.Open(driven.LogRef("topic", 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "readable" {
		t.Errorf("read %q, want the block's contents", content)
	}
}

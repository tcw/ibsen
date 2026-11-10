package wiring

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

func TestTheFenceRefusesWritesOnceTheLockIsGone(t *testing.T) {
	var held atomic.Bool
	held.Store(true)
	store := fence(memstore.New(), held.Load)
	ref := driven.LogRef("t", 0)
	if _, err := store.Append(ref, []byte("before")); err != nil {
		t.Fatal(err)
	}
	held.Store(false)
	if _, err := store.Append(ref, []byte("after")); !errors.Is(err, ErrNotHolding) {
		t.Errorf("append got %v, want ErrNotHolding", err)
	}
	if err := store.Truncate(ref, 0); !errors.Is(err, ErrNotHolding) {
		t.Errorf("truncate got %v, want ErrNotHolding", err)
	}
	if err := store.Remove(ref); !errors.Is(err, ErrNotHolding) {
		t.Errorf("remove got %v, want ErrNotHolding", err)
	}
	if _, err := store.CreateTopic("u"); !errors.Is(err, ErrNotHolding) {
		t.Errorf("create topic got %v, want ErrNotHolding", err)
	}
	// reads go on
	blocks, err := store.List("t", driven.Log)
	if err != nil || len(blocks) != 1 || blocks[0].Size != 6 {
		t.Fatalf("after refusals the block is %v, %v; want the 6 bytes from before", blocks, err)
	}
}

// Invariant 8: a decorator forwards Syncable, or the core stops flushing without a word.
func TestTheFenceForwardsSyncableExactlyWhenTheStoreHasIt(t *testing.T) {
	always := func() bool { return true }
	if _, ok := fence(filestore.NewOS(t.TempDir()), always).(driven.Syncable); !ok {
		t.Error("a fenced filestore is not Syncable")
	}
	if _, ok := fence(memstore.New(), always).(driven.Syncable); ok {
		t.Error("a fenced memstore claims to be Syncable")
	}
	dir := t.TempDir()
	store := fence(filestore.NewOS(dir), always)
	if _, err := store.Append(driven.LogRef("t", 0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if synced, err := driven.Sync(store, driven.LogRef("t", 0)); !synced || err != nil {
		t.Errorf("driven.Sync through the fence: synced=%v, %v", synced, err)
	}
}

// losesDuringAppend is a store whose lock is lost while an append is under way, and which can
// be told to fail the rollback.
type losesDuringAppend struct {
	driven.BlockStore
	held         *atomic.Bool
	truncateFail bool
}

func (l *losesDuringAppend) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	block, err := l.BlockStore.Append(ref, data)
	l.held.Store(false)
	return block, err
}

func (l *losesDuringAppend) Truncate(ref driven.BlockRef, size int64) error {
	if l.truncateFail {
		return errors.New("EIO")
	}
	return l.BlockStore.Truncate(ref, size)
}

func TestAnAppendThatOutlivesTheLockIsRolledBackOrMarkedDirty(t *testing.T) {
	for _, truncateFail := range []bool{false, true} {
		var held atomic.Bool
		held.Store(true)
		inner := &losesDuringAppend{BlockStore: memstore.New(), held: &held, truncateFail: truncateFail}
		ref := driven.LogRef("t", 0)
		if _, err := inner.BlockStore.Append(ref, []byte("kept")); err != nil {
			t.Fatal(err)
		}
		_, err := fence(inner, held.Load).Append(ref, []byte("too late"))
		if !errors.Is(err, ErrNotHolding) {
			t.Fatalf("got %v, want ErrNotHolding", err)
		}
		blocks, _ := inner.List("t", driven.Log)
		if !truncateFail {
			if errors.Is(err, driven.ErrDirtyBlock) || blocks[0].Size != 4 {
				t.Errorf("a rolled-back append left %d bytes and %v", blocks[0].Size, err)
			}
			continue
		}
		if !errors.Is(err, driven.ErrDirtyBlock) {
			t.Errorf("an append that could not be rolled back got %v, want ErrDirtyBlock", err)
		}
	}
}

var _ = domain.TopicName("")

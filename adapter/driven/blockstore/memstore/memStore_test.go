package memstore

import (
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/conformance"
	"github.com/tcw/ibsen/core/port/driven"
)

func TestMemStore_Conformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driven.BlockStore { return New() })
}

// TestMemStore_IsNotSyncable is the point of this adapter: a store that offers nothing but
// the block verbs still works, and driven.Sync says so instead of failing.
func TestMemStore_IsNotSyncable(t *testing.T) {
	store := New()
	if _, isSyncable := interface{}(store).(driven.Syncable); isSyncable {
		t.Fatal("the in-memory store must not claim to sync")
	}
	if _, err := store.Append(driven.LogRef("topic", 0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	synced, err := driven.Sync(store, driven.LogRef("topic", 0))
	if err != nil || synced {
		t.Fatalf("Sync: synced=%v, err=%v", synced, err)
	}
}

package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/port/driven"
)

// heldSyncStore is a Syncable store whose syncs wait until the test lets them go, and which
// counts the log appends and syncs that reach it.
type heldSyncStore struct {
	driven.BlockStore
	release chan struct{}
	appends atomic.Int32
	syncs   atomic.Int32
}

func (s *heldSyncStore) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	block, err := s.BlockStore.Append(ref, data)
	if err == nil && ref.Kind == driven.Log {
		s.appends.Add(1)
	}
	return block, err
}

func (s *heldSyncStore) Sync(driven.BlockRef) error {
	<-s.release
	s.syncs.Add(1)
	return nil
}

// Writers to one topic that arrive while a flush is running must be able to append behind
// it and share the next one. The manager used to hold a mutex per topic across the whole of
// Topic.Write, flush included, so a second writer could not even append until the first had
// been acknowledged, and every write through a server cost a sync of its own whatever the
// flush policy said.
func TestManager_concurrentWritersToOneTopicShareAFlush(t *testing.T) {
	const writers = 8
	store := &heldSyncStore{BlockStore: memstore.New(), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	m := newTestManagerWithStore(t, store)
	// registered after the manager's, so it runs first: a failed test must still let the
	// writers go, or closing the manager waits for them forever
	t.Cleanup(release)

	errs := make(chan error, writers+1)
	write := func(i int) {
		entries := [][]byte{[]byte(fmt.Sprintf("entry-%d", i))}
		errs <- m.Write("shared", &entries)
	}

	// the first write appends, then waits on a sync the test is holding
	go write(0)
	waitFor(t, "the first write to append", func() bool { return store.appends.Load() == 1 })

	// the rest must be able to append behind it while that sync is still running
	var wg sync.WaitGroup
	for i := 1; i <= writers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); write(i) }()
	}
	waitFor(t, "every writer to append while a flush is running",
		func() bool { return store.appends.Load() == writers+1 })

	release()
	wg.Wait()
	for i := 0; i <= writers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// one sync for the first write, one for everything that queued behind it
	if got := store.syncs.Load(); got != 2 {
		t.Fatalf("%d writes took %d syncs, want 2", writers+1, got)
	}
	got, err := readTopic(m, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != writers+1 {
		t.Fatalf("read %d entries back, want %d", len(got), writers+1)
	}
	seen := map[string]bool{}
	for i, entry := range got {
		if entry.Offset != uint64(i) {
			t.Fatalf("entry %d has offset %d", i, entry.Offset)
		}
		seen[string(entry.Entry)] = true
	}
	if len(seen) != writers+1 {
		t.Fatalf("read back %d distinct entries, want %d", len(seen), writers+1)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

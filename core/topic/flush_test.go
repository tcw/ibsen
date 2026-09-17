package topic

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// syncGate is a Syncable store whose Sync the test decides the timing and outcome of.
type syncGate struct {
	driven.BlockStore
	mu      sync.Mutex
	calls   int
	err     error
	release chan struct{}
}

func newSyncGate() *syncGate {
	return &syncGate{BlockStore: memstore.New()}
}

func (g *syncGate) Sync(driven.BlockRef) error {
	g.mu.Lock()
	release, err := g.release, g.err
	g.mu.Unlock()
	if release != nil {
		<-release
	}
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	return err
}

func (g *syncGate) syncCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

func (g *syncGate) holdSyncs() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.release = make(chan struct{})
	return g.release
}

func (g *syncGate) failWith(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = err
}

func nextOffsetOf(topic *Topic) domain.Offset {
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	return topic.NextOffset
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func writeOne(topic *Topic, payload string) error {
	entries := [][]byte{[]byte(payload)}
	return topic.Write(&entries)
}

func newGatedTopic(t *testing.T, store driven.BlockStore, entries uint32, interval time.Duration) *Topic {
	t.Helper()
	topic := NewLogTopic(Params{
		Store: store, TopicName: "t", MaxBlockSize: 1 << 20,
		FlushEntries: entries, FlushInterval: interval,
	})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return topic
}

// The whole point: an entry that is written but not yet on durable media must not be
// readable, because a power cut would take it back.
func TestReadersDoNotSeeAnUnflushedEntry(t *testing.T) {
	store := newSyncGate()
	release := store.holdSyncs()
	topic := newGatedTopic(t, store, 1, 0)

	written := make(chan error, 1)
	go func() { written <- writeOne(topic, "one") }()

	// the append has happened; the flush covering it has not
	eventually(t, "the entry to be appended", func() bool { return nextOffsetOf(topic) == 1 })
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, domain.NoEntriesFound) {
		t.Fatalf("a read saw an unflushed entry: %v", err)
	}
	select {
	case err := <-written:
		t.Fatalf("Write returned %v before its flush finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	if err := <-written; err != nil {
		t.Fatalf("write failed: %v", err)
	}
	got, err := readAllFrom(topic, 0, 10)
	if err != nil {
		t.Fatalf("read after the flush: %v", err)
	}
	if len(got) != 1 || string(got[0].Entry) != "one" {
		t.Fatalf("read %d entries after the flush, want the one written", len(got))
	}
}

// A flush that fails is reported to the writer, and its entries stay invisible: the client
// does not know whether they landed, so nothing may read them as committed.
func TestAFailedFlushIsReportedAndLeavesNothingReadable(t *testing.T) {
	store := newSyncGate()
	topic := newGatedTopic(t, store, 1, 0)
	diskOnFire := errors.New("disk on fire")
	store.failWith(diskOnFire)

	if err := writeOne(topic, "one"); !errors.Is(err, diskOnFire) {
		t.Fatalf("write returned %v, want the sync failure", err)
	}
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, domain.NoEntriesFound) {
		t.Fatalf("a read saw an entry whose flush failed: %v", err)
	}
	if topic.durableOffset() != 0 {
		t.Errorf("durable offset advanced to %d over a failed flush", topic.durableOffset())
	}
}

// The entries a failed flush left behind are written, just not known to be durable. A later
// flush covers them, and then they are readable.
func TestEntriesFromAFailedFlushBecomeReadableOnceOneSucceeds(t *testing.T) {
	store := newSyncGate()
	topic := newGatedTopic(t, store, 1, 0)
	store.failWith(errors.New("disk on fire"))
	if err := writeOne(topic, "one"); err == nil {
		t.Fatal("the first write should have failed")
	}

	store.failWith(nil)
	if err := writeOne(topic, "two"); err != nil {
		t.Fatalf("the second write failed: %v", err)
	}

	got, err := readAllFrom(topic, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Entry) != "one" || string(got[1].Entry) != "two" {
		t.Fatalf("got %d entries, want both once a flush succeeded", len(got))
	}
}

// Writers that arrive while a batch is forming share one sync rather than each paying for
// their own.
func TestConcurrentWritersShareOneFlush(t *testing.T) {
	const writers = 4
	store := newSyncGate()
	topic := newGatedTopic(t, store, writers, time.Minute)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = writeOne(topic, "e")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d failed: %v", i, err)
		}
	}
	if calls := store.syncCalls(); calls != 1 {
		t.Errorf("%d writers cost %d syncs, want 1", writers, calls)
	}
	if topic.durableOffset() != writers {
		t.Errorf("durable offset is %d after %d writes", topic.durableOffset(), writers)
	}
}

// A lone writer must not wait for a batch that will never fill: the interval releases it.
func TestTheIntervalReleasesAWriterThatNeverReachesTheThreshold(t *testing.T) {
	const interval = 80 * time.Millisecond
	store := newSyncGate()
	topic := newGatedTopic(t, store, 1000, interval)

	started := time.Now()
	if err := writeOne(topic, "alone"); err != nil {
		t.Fatal(err)
	}
	waited := time.Since(started)

	if waited < interval/2 {
		t.Errorf("the write returned after %v, so it did not wait for the %v interval", waited, interval)
	}
	if calls := store.syncCalls(); calls != 1 {
		t.Errorf("the write cost %d syncs, want 1", calls)
	}
	if topic.durableOffset() != 1 {
		t.Errorf("durable offset is %d after the interval elapsed", topic.durableOffset())
	}
}

// A store that cannot sync has nothing to push, so none of the waiting applies: what Append
// returned is as durable as it gets.
func TestAStoreThatCannotSyncNeverWaits(t *testing.T) {
	topic := newGatedTopic(t, memstore.New(), 1000, time.Minute)
	if topic.flush.syncable {
		t.Fatal("memstore should not be Syncable")
	}

	started := time.Now()
	if err := writeOne(topic, "one"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Fatalf("a write to a store that cannot sync waited %v", waited)
	}
	if topic.durableOffset() != 1 {
		t.Errorf("durable offset is %d, want the write to count immediately", topic.durableOffset())
	}
	got, err := readAllFrom(topic, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("read %d entries (%v), want the one written", len(got), err)
	}
}

// Reloading a topic trusts what the recovered block holds: it is already on the media.
func TestALoadedTopicCountsItsBlockAsDurable(t *testing.T) {
	store := newSyncGate()
	topic := newGatedTopic(t, store, 1, 0)
	for i := 0; i < 3; i++ {
		if err := writeOne(topic, "e"); err != nil {
			t.Fatal(err)
		}
	}

	reloaded := newGatedTopic(t, store, 1, 0)
	if reloaded.durableOffset() != 3 {
		t.Errorf("a reloaded topic has durable offset %d, want 3", reloaded.durableOffset())
	}
	got, err := readAllFrom(reloaded, 0, 10)
	if err != nil || len(got) != 3 {
		t.Fatalf("read %d entries (%v) from a reloaded topic, want 3", len(got), err)
	}
}

package topic

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/port/driven"
)

// armedIndexGate holds the first index append after it is armed, so a test can keep an
// indexing run still and watch what becomes of a request that arrives while it is stuck.
type armedIndexGate struct {
	driven.BlockStore
	armed   atomic.Bool
	appends atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newArmedIndexGate() *armedIndexGate {
	return &armedIndexGate{
		BlockStore: memstore.New(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (g *armedIndexGate) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	if ref.Kind == driven.Index {
		g.appends.Add(1)
		if g.armed.Load() {
			g.once.Do(func() {
				close(g.entered)
				<-g.release
			})
		}
	}
	return g.BlockStore.Append(ref, data)
}

// The bug this pins: a caller that found indexing already under way had its request dropped,
// because the exclusion flag was released after the topic lock and a write landing in between
// saw a run that was on its way out. Nothing noticed until a ten-second sweep over every
// loaded topic came round. A run now takes that work before it stops, and there is no sweep.
func TestIndexingTakesUpWorkLeftWhileItRan(t *testing.T) {
	gate := newArmedIndexGate()
	topic := newTestTopic(t, gate, 1<<20)
	writeEntries(t, topic, 0, 5)

	gate.armed.Store(true)
	gate.appends.Store(0)

	ran := make(chan bool, 1)
	go func() {
		took, err := topic.UpdateIndex()
		if err != nil {
			t.Error(err)
		}
		ran <- took
	}()
	<-gate.entered // the run is stuck inside the store, holding the flag

	// a request arriving now finds the run under way and leaves it the work
	if took, err := topic.UpdateIndex(); err != nil || took {
		t.Fatalf("the second call reported took=%v err=%v, want false: one run at a time", took, err)
	}

	close(gate.release)
	if !<-ran {
		t.Error("the first call did not report that it did the indexing")
	}

	if got := gate.appends.Load(); got != 2 {
		t.Errorf("the index ran %d times, want 2: the run must take up the work left for it", got)
	}
}

// Whoever is already indexing covers everyone who arrives while it works, so a crowd of
// callers is one run plus whatever arrived during it, not a run each.
func TestOnlyOneIndexRunsAtATime(t *testing.T) {
	topic := newTestTopic(t, memstore.New(), 1<<20)
	writeEntries(t, topic, 0, 50)

	var took atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ran, err := topic.UpdateIndex()
			if err != nil {
				t.Error(err)
			}
			if ran {
				took.Add(1)
			}
		}()
	}
	wg.Wait()

	if took.Load() == 0 {
		t.Error("nobody indexed")
	}
	// whatever the interleaving, the index must match what a clean scan of the log gives
	assertIndexMatchesFullScan(t, topic)
}

// A failed run leaves the work outstanding rather than swallowing it, so the next write picks
// it up. It does not retry on the spot, which would spin on a store that is failing — the
// rule a failed flush already follows.
func TestAFailedIndexRunLeavesTheWorkForTheNextCall(t *testing.T) {
	store := &failingIndexStore{BlockStore: memstore.New()}
	topic := newTestTopic(t, store, 1<<20)

	store.fail.Store(true)
	batch := payloads(0, 3)
	if err := topic.Write(&batch); err != nil {
		t.Fatal(err)
	}
	topic.indexWg.Wait()
	if _, err := topic.UpdateIndex(); err == nil {
		t.Fatal("indexing succeeded while the store was failing")
	}

	store.fail.Store(false)
	if _, err := topic.UpdateIndex(); err != nil {
		t.Fatal(err)
	}

	assertIndexMatchesFullScan(t, topic)
}

// failingIndexStore fails every append to an index block while armed.
type failingIndexStore struct {
	driven.BlockStore
	fail atomic.Bool
}

func (s *failingIndexStore) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	if ref.Kind == driven.Index && s.fail.Load() {
		return driven.Block{}, errInjected
	}
	return s.BlockStore.Append(ref, data)
}

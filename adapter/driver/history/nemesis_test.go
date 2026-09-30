package history_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driver/history"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
)

// These tests are the Jepsen shape on one machine: concurrent clients write and read through
// the driving port while a nemesis cuts the power under them, a few lives in a row, and the
// checker then decides whether the log read back from the directory explains everything every
// client was told. The nemesis is faultfs.PageCache, which knows what reached the media, so a
// crash loses what a power cut loses rather than what a killed process loses.

// scenario is one kind of trouble, run over several seeds.
type scenario struct {
	name string
	// loss decides what that was not synced survives each crash
	loss func(seed uint64) faultfs.Loss
	// failSyncs, when set, makes one fsync of a log block fail at a random point of each life
	failSyncs bool
	// the flush policy under test; zero is the default, every write synced before it is acked
	flushEntries  uint32
	flushInterval time.Duration
}

const (
	writers        = 4
	readers        = 2
	lives          = 3
	seedsPerRun    = 12
	topicsInPlay   = 2
	maxEntriesSent = 6
)

func TestNemesis(t *testing.T) {
	for _, s := range []scenario{
		{name: "power cut loses everything unsynced",
			loss: func(uint64) faultfs.Loss { return faultfs.LoseEverything() }},
		{name: "power cut keeps some of what was unsynced",
			loss: faultfs.LoseSome},
		{name: "power cut under coalesced flushes",
			loss: faultfs.LoseSome, flushEntries: 20, flushInterval: time.Millisecond},
		{name: "fsync fails, then power cut",
			loss:      func(uint64) faultfs.Loss { return faultfs.LoseEverything() },
			failSyncs: true},
	} {
		t.Run(s.name, func(t *testing.T) {
			for seed := uint64(1); seed <= seedsPerRun; seed++ {
				runSeed(t, s, seed)
			}
		})
	}
}

func runSeed(t *testing.T, s scenario, seed uint64) {
	t.Helper()
	dir := t.TempDir()
	cache := faultfs.NewPageCache()
	h := &history.History{}
	nemesis := rand.New(rand.NewPCG(seed, 1))
	loss := s.loss(seed)

	for life := 0; life < lives; life++ {
		m := newManager(t, filestore.New(cache, dir), s)
		crashAt := int64(20 + nemesis.IntN(120))
		failAt := int64(-1)
		if s.failSyncs {
			failAt = int64(nemesis.IntN(int(crashAt)))
		}
		var writes atomic.Int64
		// every writer calls this after each write; the one that reaches a threshold brings
		// the trouble, while the others are wherever they are. Reads are not counted: an
		// empty one is so quick that they would use up the budget before a write finished.
		afterWrite := func() {
			switch writes.Add(1) {
			case failAt:
				cache.FailSyncs(".log", 1)
			case crashAt:
				cache.Crash()
			}
		}

		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			process := life*100 + w
			rng := rand.New(rand.NewPCG(seed, uint64(process)))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for sent := 0; !cache.Crashed(); sent++ {
					topic := domain.TopicName(fmt.Sprintf("topic-%d", rng.IntN(topicsInPlay)))
					entries := make([]string, 1+rng.IntN(maxEntriesSent))
					for i := range entries {
						entries[i] = fmt.Sprintf("p%d-w%d-e%d", process, sent, i)
					}
					_ = h.Write(m, process, topic, entries)
					afterWrite()
				}
			}()
		}
		for r := 0; r < readers; r++ {
			process := life*100 + 50 + r
			rng := rand.New(rand.NewPCG(seed, uint64(process)))
			wg.Add(1)
			go func() {
				defer wg.Done()
				seen := map[domain.TopicName]int{}
				for !cache.Crashed() {
					topic := domain.TopicName(fmt.Sprintf("topic-%d", rng.IntN(topicsInPlay)))
					got, err := h.Read(m, process, topic, uint64(rng.IntN(seen[topic]+1)))
					if err == nil && len(got) > 0 {
						seen[topic] = int(got[len(got)-1].Offset)
					}
				}
			}()
		}
		wg.Wait()
		m.Close()
		if err := cache.Recover(loss); err != nil {
			t.Fatal(err)
		}
	}

	final, loadErr := readBack(t, dir)
	if loadErr != nil {
		t.Errorf("seed %d: the log could not be read after the last crash: %v", seed, loadErr)
		return
	}
	ops := h.Ops()
	if acked := countAcked(ops); acked == 0 {
		t.Fatalf("seed %d: no write was acknowledged, so nothing was tested", seed)
	}
	if anomalies := history.Check(ops, final); len(anomalies) > 0 {
		t.Errorf("seed %d: %d anomalies, first %s", seed, len(anomalies), summarize(anomalies))
	}
}

func newManager(t *testing.T, store *filestore.Store, s scenario) *manager.LogTopicsManager {
	t.Helper()
	m, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store: store,
		// small blocks and frames, so a life rolls over to new blocks and a write is several
		// frames, which is where the directory syncs and the torn prefixes are
		MaxBlockSize:    2000,
		MaxFrameEntries: 3,
		FlushEntries:    s.flushEntries,
		FlushInterval:   s.flushInterval,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &m
}

// readBack opens the directory as a restarted server would, on the real filesystem, and reads
// every topic from the start.
func readBack(t *testing.T, dir string) (map[domain.TopicName][]history.Entry, error) {
	t.Helper()
	m, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store: filestore.NewOS(dir), MaxBlockSize: 2000, MaxFrameEntries: 3,
	})
	if err != nil {
		return nil, err
	}
	defer m.Close()
	final := map[domain.TopicName][]history.Entry{}
	for _, topic := range m.List() {
		entries, err := history.ReadAll(&m, topic, 0)
		if err != nil {
			return nil, fmt.Errorf("topic %s: %w", topic, err)
		}
		final[topic] = entries
	}
	return final, nil
}

func countAcked(ops []history.Op) int {
	acked := 0
	for _, op := range ops {
		if op.Kind == history.Write && op.Err == nil {
			acked++
		}
	}
	return acked
}

func summarize(anomalies []history.Anomaly) string {
	kinds := map[string]int{}
	for _, anomaly := range anomalies {
		kinds[anomaly.Kind]++
	}
	var parts []string
	for kind, n := range kinds {
		parts = append(parts, fmt.Sprintf("%d× %s", n, kind))
	}
	return anomalies[0].String() + " (" + strings.Join(parts, ", ") + ")"
}

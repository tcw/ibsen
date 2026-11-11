package topic

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// Lenient mode acknowledges a write once it is appended and leaves the flush to whoever calls
// Flush. What does not change is the reader's side: nothing is readable before it is durable.

func newLenientTopic(t *testing.T, store driven.BlockStore, logger driven.Logger) *Topic {
	t.Helper()
	topic := NewLogTopic(Params{
		Store: store, TopicName: "t", MaxBlockSize: 1 << 20, FlushMode: FlushLenient, Logger: logger,
	})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestLenient_aWriteReturnsBeforeAnySync(t *testing.T) {
	store := newSyncGate()
	release := store.holdSyncs()
	topic := newLenientTopic(t, store, nil)

	written := make(chan error, 1)
	go func() { written <- writeOne(topic, "one") }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a lenient write waited for a sync")
	}
	if calls := store.syncCalls(); calls != 0 {
		t.Errorf("a lenient write synced %d times", calls)
	}
	close(release)
}

func TestLenient_nothingIsReadableUntilFlush(t *testing.T) {
	store := newSyncGate()
	topic := newLenientTopic(t, store, nil)
	for i := 0; i < 3; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, domain.NoEntriesFound) {
		t.Fatalf("an acknowledged but unflushed entry was readable: %v", err)
	}

	// a flush held in its sync makes nothing readable either
	release := store.holdSyncs()
	flushed := make(chan error, 1)
	go func() { flushed <- topic.Flush() }()
	time.Sleep(20 * time.Millisecond)
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, domain.NoEntriesFound) {
		t.Fatalf("an entry was readable while its flush was still syncing: %v", err)
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}

	got, err := readAllFrom(topic, 0, 10)
	if err != nil || len(got) != 3 {
		t.Fatalf("after the flush read %d entries, %v; want 3", len(got), err)
	}
	if calls := store.syncCalls(); calls != 1 {
		t.Errorf("three writes and a flush made %d syncs, want 1", calls)
	}
}

func TestLenient_aTailingReaderSeesEntriesAfterTheTick(t *testing.T) {
	store := newSyncGate()
	topic := newLenientTopic(t, store, nil)

	// the composition root's ticker, played by the test
	stop := make(chan struct{})
	var ticking sync.WaitGroup
	ticking.Add(1)
	go func() {
		defer ticking.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				_ = topic.Flush()
			}
		}
	}()
	t.Cleanup(func() { close(stop); ticking.Wait() })

	const n = 50
	for i := 0; i < n; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	var seen int
	eventually(t, "a tailing reader to see every entry", func() bool {
		got, err := readAllFrom(topic, domain.Offset(seen), 100)
		if err == nil {
			if checkErr := offsetsFrom(got, seen); checkErr != nil {
				t.Fatal(checkErr)
			}
			seen += len(got)
		}
		return seen == n
	})
}

func offsetsFrom(got []domain.LogEntry, from int) error {
	for i, entry := range got {
		if entry.Offset != uint64(from+i) || string(entry.Entry) != fmt.Sprint(from+i) {
			return fmt.Errorf("position %d holds offset %d %q", from+i, entry.Offset, entry.Entry)
		}
	}
	return nil
}

func TestLenient_concurrentWritersAllLandAndOneFlushCoversThem(t *testing.T) {
	store := newSyncGate()
	topic := newLenientTopic(t, store, nil)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := writeOne(topic, "x"); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := topic.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := readAllFrom(topic, 0, 1000)
	if err != nil || len(got) != 400 {
		t.Fatalf("read %d entries, %v; want 400", len(got), err)
	}
	for i, entry := range got {
		if entry.Offset != uint64(i) {
			t.Fatalf("position %d holds offset %d", i, entry.Offset)
		}
	}
	if calls := store.syncCalls(); calls != 1 {
		t.Errorf("400 writes and one flush made %d syncs, want 1", calls)
	}
}

func TestLenient_closeFlushes(t *testing.T) {
	store := newSyncGate()
	topic := newLenientTopic(t, store, nil)
	if err := writeOne(topic, "last words"); err != nil {
		t.Fatal(err)
	}
	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}
	if calls := store.syncCalls(); calls != 1 {
		t.Errorf("close made %d syncs, want 1", calls)
	}
	if durable := topic.durableOffset(); durable != 1 {
		t.Errorf("after close the durable offset is %d, want 1", durable)
	}
}

// A failed fsync in lenient mode: the writes it covered were already acknowledged, so the
// failure is logged with what it took, and every later call on the topic says so.
func TestLenient_aFailedFlushStopsTheTopicAndIsReportedEverywhere(t *testing.T) {
	store := newSyncGate()
	logger := &recordingLogger{}
	topic := newLenientTopic(t, store, logger)
	for i := 0; i < 2; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	store.failWith(errors.New("EIO"))

	if err := topic.Flush(); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("flush got %v, want ErrFlushFailed", err)
	}
	if err := writeOne(topic, "after"); !errors.Is(err, ErrFlushFailed) {
		t.Errorf("the next write got %v, want ErrFlushFailed", err)
	}
	if err := topic.Flush(); !errors.Is(err, ErrFlushFailed) {
		t.Errorf("the next flush got %v, want ErrFlushFailed", err)
	}
	if err := topic.Close(); !errors.Is(err, ErrFlushFailed) {
		t.Errorf("close got %v, want ErrFlushFailed", err)
	}
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, domain.NoEntriesFound) {
		t.Errorf("an entry past the durable offset became readable: %v", err)
	}

	logged, ok := logger.find("a flush failed: acknowledged entries may be lost to a power cut, and the topic takes no more writes")
	if !ok || logged.level != driven.LevelError {
		t.Fatalf("the failure was not logged at error level: %+v", logged)
	}
	from, _ := field(logged, "fromOffset")
	to, _ := field(logged, "toOffset")
	if from.Uint != 0 || to.Uint != 2 {
		t.Errorf("logged the range [%d, %d), want [0, 2)", from.Uint, to.Uint)
	}
}

// The power-cut model: what was flushed survives, what was only acknowledged may not, the
// offsets stay contiguous, and no reader ever saw what was lost.
func TestLenient_aPowerCutLosesOnlyWhatWasNotFlushed(t *testing.T) {
	params := Params{MaxBlockSize: 1 << 20, FlushMode: FlushLenient}
	topic, cache, dir := newPowerCutTopic(t, params)
	for i := 0; i < 5; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := topic.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 5; i < 9; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	seen, err := readPayloads(t, topic)
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := afterPowerCut(t, topic, cache, faultfs.LoseEverything(), dir, params)
	if err != nil {
		t.Fatal(err)
	}
	survived, err := readPayloads(t, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if len(survived) != 5 {
		t.Errorf("%d entries survived, want the 5 that were flushed: %v", len(survived), survived)
	}
	for i, payload := range survived {
		if payload != fmt.Sprint(i) {
			t.Fatalf("offset %d holds %q after recovery", i, payload)
		}
	}
	for i, payload := range seen {
		if i >= len(survived) || survived[i] != payload {
			t.Fatalf("a reader saw %q at offset %d, which the power cut took", payload, i)
		}
	}

	// the log carries on from where the media says it ends
	if err := writeOne(recovered, "5"); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Flush(); err != nil {
		t.Fatal(err)
	}
	again, _ := readPayloads(t, recovered)
	if len(again) != 6 || again[5] != "5" {
		t.Errorf("after recovery and one more write the log holds %v", again)
	}
}

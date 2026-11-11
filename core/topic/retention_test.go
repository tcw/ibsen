package topic

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// agedStore reports the modification times a test gives it, so a block can be any age.
type agedStore struct {
	driven.BlockStore
	mu   sync.Mutex
	ages map[uint64]time.Time
}

func (s *agedStore) setModified(block uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ages[block] = at
}

func (s *agedStore) List(topic domain.TopicName, kind driven.BlockKind) ([]driven.Block, error) {
	blocks, err := s.BlockStore.List(topic, kind)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range blocks {
		if at, ok := s.ages[blocks[i].Block]; ok && kind == driven.Log {
			blocks[i].Modified = at
		}
	}
	return blocks, err
}

// newRetentionTopic writes one entry per block, so blocks and offsets line up: block i starts
// at offset i. The block size is tiny, so every write after the first rolls over.
func newRetentionTopic(t *testing.T, blocks int) (*Topic, *agedStore) {
	t.Helper()
	store := &agedStore{BlockStore: memstore.New(), ages: map[uint64]time.Time{}}
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { topic.Close() })
	for i := 0; i < blocks; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(topic.LogBlockList); got != blocks {
		t.Fatalf("%d writes made %d blocks, want one each", blocks, got)
	}
	return topic, store
}

func blockSizes(t *testing.T, topic *Topic) (int64, int) {
	t.Helper()
	blocks, err := topic.Store.List("t", driven.Log)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, block := range blocks {
		total += block.Size
	}
	return total, len(blocks)
}

func storedBlocks(t *testing.T, store driven.BlockStore, kind driven.BlockKind) []uint64 {
	t.Helper()
	blocks, err := store.List("t", kind)
	if err != nil {
		t.Fatal(err)
	}
	var names []uint64
	for _, block := range blocks {
		names = append(names, block.Block)
	}
	return names
}

func TestRetain_byAgeDeletesTheOldBlocksOnly(t *testing.T) {
	topic, store := newRetentionTopic(t, 6)
	now := time.Now()
	for block := uint64(0); block < 6; block++ {
		// blocks 0..2 are two days old, 3..5 an hour old
		age := time.Hour
		if block < 3 {
			age = 48 * time.Hour
		}
		store.setModified(block, now.Add(-age))
	}
	removed, err := topic.Retain(RetentionPolicy{MaxAge: 24 * time.Hour}, now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(removed.Blocks) != "[0 1 2]" || removed.Earliest != 3 {
		t.Fatalf("removed %v, earliest %d; want [0 1 2] and 3", removed.Blocks, removed.Earliest)
	}
	if got := fmt.Sprint(storedBlocks(t, store, driven.Log)); got != "[3 4 5]" {
		t.Errorf("log blocks left %s", got)
	}
	for _, index := range storedBlocks(t, store, driven.Index) {
		if index < 3 {
			t.Errorf("the index of removed block %d is still there", index)
		}
	}
}

// Deletion is a prefix: a young block in front of an old one stops it, or the offsets left
// would have a hole.
func TestRetain_byAgeStopsAtTheFirstYoungBlock(t *testing.T) {
	topic, store := newRetentionTopic(t, 4)
	now := time.Now()
	store.setModified(0, now.Add(-48*time.Hour))
	store.setModified(1, now)
	store.setModified(2, now.Add(-48*time.Hour))
	removed, err := topic.Retain(RetentionPolicy{MaxAge: time.Hour}, now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(removed.Blocks) != "[0]" {
		t.Fatalf("removed %v, want only block 0", removed.Blocks)
	}
}

func TestRetain_aBlockOfUnknownAgeIsNeverOld(t *testing.T) {
	topic, store := newRetentionTopic(t, 3)
	store.setModified(0, time.Time{})
	removed, err := topic.Retain(RetentionPolicy{MaxAge: time.Nanosecond}, time.Now().Add(time.Hour))
	if err != nil || len(removed.Blocks) != 0 {
		t.Fatalf("removed %v, %v; a block whose store cannot tell its age must stay", removed.Blocks, err)
	}
}

func TestRetain_bySizeKeepsTheTopicUnderTheLimit(t *testing.T) {
	topic, store := newRetentionTopic(t, 8)
	total, _ := blockSizes(t, topic)
	perBlock := total / 8
	limit := perBlock*3 + perBlock/2
	removed, err := topic.Retain(RetentionPolicy{MaxBytes: limit}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	left, count := blockSizes(t, topic)
	if left > limit || count != 3 {
		t.Fatalf("%d bytes in %d blocks left, want at most %d in 3", left, count, limit)
	}
	if fmt.Sprint(removed.Blocks) != "[0 1 2 3 4]" || removed.Bytes != total-left {
		t.Errorf("removed %v (%d bytes)", removed.Blocks, removed.Bytes)
	}
	_ = store
}

func TestRetain_eitherLimitDeletes(t *testing.T) {
	topic, store := newRetentionTopic(t, 6)
	now := time.Now()
	for block := uint64(0); block < 6; block++ {
		store.setModified(block, now)
	}
	store.setModified(0, now.Add(-48*time.Hour))
	total, _ := blockSizes(t, topic)
	// age takes block 0; size alone would take 0 and 1
	removed, err := topic.Retain(RetentionPolicy{MaxAge: time.Hour, MaxBytes: total * 4 / 6}, now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(removed.Blocks) != "[0 1]" {
		t.Fatalf("removed %v, want [0 1]", removed.Blocks)
	}
}

func TestRetain_neverTheHead(t *testing.T) {
	topic, store := newRetentionTopic(t, 3)
	now := time.Now().Add(1000 * time.Hour)
	removed, err := topic.Retain(RetentionPolicy{MaxAge: time.Nanosecond, MaxBytes: 1}, now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(removed.Blocks) != "[0 1]" {
		t.Fatalf("removed %v, want everything but the head", removed.Blocks)
	}
	again, err := topic.Retain(RetentionPolicy{MaxAge: time.Nanosecond, MaxBytes: 1}, now)
	if err != nil || len(again.Blocks) != 0 {
		t.Fatalf("a topic of one block lost %v, %v", again.Blocks, err)
	}
	if got := fmt.Sprint(storedBlocks(t, store, driven.Log)); got != "[2]" {
		t.Errorf("blocks left %s, want the head", got)
	}
	// and the head still takes writes
	if err := writeOne(topic, "3"); err != nil {
		t.Fatal(err)
	}
	got, err := readAllFrom(topic, 2, 10)
	if err != nil || len(got) != 2 || got[1].Offset != 3 {
		t.Fatalf("after retention read %v, %v", got, err)
	}
}

func TestRetain_aReadOfADeletedOffsetNamesTheEarliest(t *testing.T) {
	topic, _ := newRetentionTopic(t, 5)
	if _, err := topic.Retain(RetentionPolicy{MaxBytes: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err := readAllFrom(topic, 1, 10)
	var tooOld *domain.OffsetTooOldError
	if !errors.As(err, &tooOld) || !errors.Is(err, domain.ErrOffsetTooOld) {
		t.Fatalf("a read of a deleted offset got %v, want ErrOffsetTooOld", err)
	}
	if tooOld.Requested != 1 || tooOld.Earliest != 4 {
		t.Errorf("got %+v, want offset 1 asked for and 4 the earliest", tooOld)
	}
}

func TestRetain_aReadFromEarliestStartsAtWhatIsLeft(t *testing.T) {
	topic, _ := newRetentionTopic(t, 5)
	if _, err := topic.Retain(RetentionPolicy{MaxBytes: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := readFrom(topic, domain.ReadLogParams{From: 0, BatchSize: 10, FromEarliest: true})
	if err != nil || len(got) != 1 || got[0].Offset != 4 || string(got[0].Entry) != "4" {
		t.Fatalf("read from earliest got %v, %v; want offset 4", got, err)
	}
	// an offset that is still there is read from where it was asked for
	got, err = readFrom(topic, domain.ReadLogParams{From: 4, BatchSize: 10, FromEarliest: true})
	if err != nil || len(got) != 1 || got[0].Offset != 4 {
		t.Fatalf("got %v, %v", got, err)
	}
}

// A reader part way through the log whose next block is deleted under it is told so, rather
// than skipped ahead to whatever comes after.
func TestRetain_aReaderWhoseNextBlockIsDeletedIsToldSo(t *testing.T) {
	topic, _ := newRetentionTopic(t, 4)
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	readErr := make(chan error, 1)
	go func() {
		readErr <- topic.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: 0, BatchSize: 1})
		close(logChan)
	}()
	first := <-logChan // the read holds its snapshot and has sent block 0
	if (*first)[0].Offset != 0 {
		t.Fatalf("first batch %v", *first)
	}
	if _, err := topic.Retain(RetentionPolicy{MaxBytes: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	wg.Done()
	// the reader may have opened block 1 before it went, and an open block is still read; it
	// may not skip from there to block 3
	next := uint64(1)
	for batch := range logChan {
		for _, entry := range *batch {
			if entry.Offset != next {
				t.Errorf("the read skipped from %d to %d", next, entry.Offset)
			}
			next = entry.Offset + 1
		}
		wg.Done()
	}
	err := <-readErr
	var tooOld *domain.OffsetTooOldError
	if !errors.As(err, &tooOld) {
		t.Fatalf("the read carried on past a deleted block: %v", err)
	}
	if uint64(tooOld.Requested) != next || tooOld.Earliest != 3 {
		t.Errorf("got %+v, want offset %d gone and 3 the earliest", tooOld, next)
	}
}

// The order is log, then index, so a crash in between leaves an index with no log. Loading
// ignores it, says so, and removes it.
func TestRetain_aCrashBetweenTheLogAndTheIndexLeavesAnOrphanLoadingIgnores(t *testing.T) {
	topic, store := newRetentionTopic(t, 4)
	// indexing runs in the background; the crash is after it got as far as block 0
	eventually(t, "block 0 to be indexed", func() bool {
		_, _ = topic.UpdateIndex()
		blocks := storedBlocks(t, store, driven.Index)
		return len(blocks) > 0 && blocks[0] == 0
	})
	// what the crash leaves: block 0's log is gone, its index is not
	if err := store.Remove(driven.LogRef("t", 0)); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(storedBlocks(t, store, driven.Index))[:2] != "[0" {
		t.Fatalf("index blocks %v, want block 0's still there", storedBlocks(t, store, driven.Index))
	}
	_ = topic

	logger := &recordingLogger{}
	reloaded := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1, Logger: logger})
	if err := reloaded.LoadOrCreate(); err != nil {
		t.Fatalf("a topic with an orphan index did not load: %v", err)
	}
	t.Cleanup(func() { reloaded.Close() })
	if _, ok := logger.find("ignoring an index block whose log block is gone"); !ok {
		t.Error("the orphan was not reported")
	}
	if blocks := storedBlocks(t, store, driven.Index); len(blocks) > 0 && blocks[0] == 0 {
		t.Errorf("the orphan index is still there: %v", blocks)
	}
	got, err := readFrom(reloaded, domain.ReadLogParams{From: 1, BatchSize: 10})
	if err != nil || len(got) != 3 || got[0].Offset != 1 {
		t.Fatalf("after the reload read %v, %v", got, err)
	}
	if _, err := readFrom(reloaded, domain.ReadLogParams{From: 0, BatchSize: 10}); !errors.Is(err, domain.ErrOffsetTooOld) {
		t.Errorf("offset 0 got %v, want ErrOffsetTooOld", err)
	}
}

func readFrom(topic *Topic, params domain.ReadLogParams) ([]domain.LogEntry, error) {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var got []domain.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got = append(got, *batch...)
			wg.Done()
		}
		close(done)
	}()
	params.LogChan = logChan
	params.Wg = &wg
	err := topic.Read(params)
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

// In lenient mode a batch waiting to be flushed can name a block that retention removes
// before the flush comes. That block was synced by the rollover barrier when it stopped being
// the head, so the flush has nothing to do for it, and must not stop the topic over it.
func TestRetain_aLenientFlushAfterRetentionDoesNotFail(t *testing.T) {
	topic := NewLogTopic(Params{Store: filestore.NewOS(t.TempDir()), TopicName: "t", MaxBlockSize: 1, FlushMode: FlushLenient})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { topic.Close() })
	for i := 0; i < 4; i++ {
		if err := writeOne(topic, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := topic.Retain(RetentionPolicy{MaxBytes: 1}, time.Now())
	if err != nil || len(removed.Blocks) != 3 {
		t.Fatalf("removed %v, %v", removed.Blocks, err)
	}
	if err := topic.Flush(); err != nil {
		t.Fatalf("a flush naming removed blocks failed: %v", err)
	}
	got, err := readFrom(topic, domain.ReadLogParams{From: 3, BatchSize: 10})
	if err != nil || len(got) != 1 {
		t.Fatalf("read %v, %v", got, err)
	}
}

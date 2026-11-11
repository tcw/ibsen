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
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
)

var (
	producerA = domain.ProducerID{0xA}
	producerB = domain.ProducerID{0xB}
)

func produce(t *testing.T, topic *Topic, producer domain.ProducerID, sequence uint64, payloads ...string) (domain.WriteResult, error) {
	t.Helper()
	entries := make([][]byte, len(payloads))
	for i, payload := range payloads {
		entries[i] = []byte(payload)
	}
	return topic.WriteWith(WriteRequest{Entries: &entries, Producer: producer, Sequence: sequence})
}

func mustProduce(t *testing.T, topic *Topic, producer domain.ProducerID, sequence uint64, payloads ...string) domain.WriteResult {
	t.Helper()
	result, err := produce(t, topic, producer, sequence, payloads...)
	if err != nil {
		t.Fatalf("sequence %d: %v", sequence, err)
	}
	return result
}

func openTopic(t *testing.T, store driven.BlockStore, params Params) *Topic {
	t.Helper()
	params.Store = store
	params.TopicName = "t"
	if params.MaxBlockSize == 0 {
		params.MaxBlockSize = 1 << 20
	}
	topic := NewLogTopic(params)
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { topic.Close() })
	return topic
}

func payloadsOf(t *testing.T, topic *Topic) []string {
	t.Helper()
	got, err := readPayloads(t, topic)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestProducer_aDuplicateReturnsTheOriginalAndAppendsNothing(t *testing.T) {
	topic := openTopic(t, memstore.New(), Params{})
	mustProduce(t, topic, producerB, 7, "other")
	first := mustProduce(t, topic, producerA, 1, "a", "b")
	if first.Duplicate || first.First != 1 || first.Count != 2 {
		t.Fatalf("first write %+v", first)
	}
	again := mustProduce(t, topic, producerA, 1, "a", "b")
	if !again.Duplicate || again.First != 1 || again.Count != 2 {
		t.Fatalf("the retry got %+v, want a duplicate at 1, 2 entries", again)
	}
	if got := fmt.Sprint(payloadsOf(t, topic)); got != "[other a b]" {
		t.Errorf("the log holds %s", got)
	}
}

func TestProducer_sequencesNextGapAndStale(t *testing.T) {
	topic := openTopic(t, memstore.New(), Params{})
	// the first write of a producer the topic does not know may carry any number
	mustProduce(t, topic, producerA, 40, "40")
	mustProduce(t, topic, producerA, 41, "41")
	if _, err := produce(t, topic, producerA, 43, "43"); !errors.Is(err, domain.ErrSequenceGap) {
		t.Errorf("a gap got %v, want ErrSequenceGap", err)
	}
	if _, err := produce(t, topic, producerA, 39, "39"); !errors.Is(err, domain.ErrStaleSequence) {
		t.Errorf("an older number got %v, want ErrStaleSequence", err)
	}
	if _, err := produce(t, topic, producerA, 41, "41", "and more"); !errors.Is(err, domain.ErrSequenceReused) {
		t.Errorf("the last number on a different write got %v, want ErrSequenceReused", err)
	}
	mustProduce(t, topic, producerA, 42, "42")
	if got := fmt.Sprint(payloadsOf(t, topic)); got != "[40 41 42]" {
		t.Errorf("the log holds %s", got)
	}
}

func TestProducer_aWriteWithoutAProducerIsUnchanged(t *testing.T) {
	topic := openTopic(t, memstore.New(), Params{})
	for i := 0; i < 2; i++ {
		if err := writeOne(topic, "same"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(payloadsOf(t, topic)); got != 2 {
		t.Errorf("two writes without a producer stored %d entries", got)
	}
}

// The table is rebuilt from frame headers, so a retry that arrives after a restart is still
// recognised.
func TestProducer_survivesARestart(t *testing.T) {
	dir := t.TempDir()
	first := openTopic(t, filestore.NewOS(dir), Params{MaxFrameEntries: 2})
	mustProduce(t, first, producerA, 1, "a")
	mustProduce(t, first, producerA, 2, "b", "c", "d", "e") // several frames
	mustProduce(t, first, producerB, 9, "z")
	first.Close()

	restarted := openTopic(t, filestore.NewOS(dir), Params{MaxFrameEntries: 2})
	retry := mustProduce(t, restarted, producerA, 2, "b", "c", "d", "e")
	if !retry.Duplicate || retry.First != 1 || retry.Count != 4 {
		t.Fatalf("a retry after a restart got %+v, want the original at 1, 4 entries", retry)
	}
	mustProduce(t, restarted, producerA, 3, "f")
	if _, err := produce(t, restarted, producerB, 11, "gap"); !errors.Is(err, domain.ErrSequenceGap) {
		t.Errorf("producer B after a restart: %v", err)
	}
	if got := fmt.Sprint(payloadsOf(t, restarted)); got != "[a b c d e z f]" {
		t.Errorf("the log holds %s", got)
	}
}

// A producer is remembered across the newest ProducerWindowBlocks blocks, and no further.
func TestProducer_isForgottenOutsideTheWindow(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			dir := t.TempDir()
			params := Params{MaxBlockSize: 1, ProducerWindowBlocks: 2}
			topic := openTopic(t, filestore.NewOS(dir), params)
			mustProduce(t, topic, producerA, 5, "a5") // block 0
			mustProduce(t, topic, producerB, 1, "b1") // block 1
			if again := mustProduce(t, topic, producerA, 5, "a5"); !again.Duplicate {
				t.Fatal("a producer one block back was forgotten")
			}
			mustProduce(t, topic, producerB, 2, "b2") // block 2: A's block falls out
			if restart {
				topic.Close()
				topic = openTopic(t, filestore.NewOS(dir), params)
			}
			// forgotten, so any number is a first write, and the retry is stored again
			if result := mustProduce(t, topic, producerA, 5, "a5"); result.Duplicate {
				t.Fatal("a producer older than the window was still remembered")
			}
		})
	}
}

func TestProducer_concurrentProducersAndOneRacingItself(t *testing.T) {
	topic := openTopic(t, memstore.New(), Params{})
	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		producer := domain.ProducerID{byte(p + 1)}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(1); seq <= 20; seq++ {
				// every write is sent twice at once, as a client that timed out might
				var pair sync.WaitGroup
				results := make([]domain.WriteResult, 2)
				for i := range results {
					pair.Add(1)
					go func() {
						defer pair.Done()
						entries := [][]byte{[]byte(fmt.Sprintf("p%d-%d", producer[0], seq))}
						var err error
						results[i], err = topic.WriteWith(WriteRequest{Entries: &entries, Producer: producer, Sequence: seq})
						if err != nil {
							t.Error(err)
						}
					}()
				}
				pair.Wait()
				if results[0].Duplicate == results[1].Duplicate || results[0].First != results[1].First {
					t.Errorf("two sends of one write: %+v and %+v; want one stored and one duplicate of it", results[0], results[1])
				}
			}
		}()
	}
	wg.Wait()
	got := payloadsOf(t, topic)
	if len(got) != 160 {
		t.Fatalf("%d entries stored, want 160", len(got))
	}
	seen := map[string]bool{}
	for _, payload := range got {
		if seen[payload] {
			t.Fatalf("%s stored twice", payload)
		}
		seen[payload] = true
	}
}

// A producer's write cut short by a crash is cut whole by recovery, so its retry is a plain
// write, stored once, rather than a "duplicate" of half of itself.
func TestProducer_aTornWriteIsCutWholeAndItsRetryStoredOnce(t *testing.T) {
	dir := t.TempDir()
	params := Params{MaxFrameEntries: 1}
	topic := openTopic(t, filestore.NewOS(dir), params)
	mustProduce(t, topic, producerA, 1, "a")
	mustProduce(t, topic, producerA, 2, "b", "c", "d") // three frames
	topic.Close()

	// the crash: the last frame of the second write never made it
	store := filestore.NewOS(dir)
	blocks, _ := store.List("t", driven.Log)
	if err := store.Truncate(driven.LogRef("t", 0), blocks[0].Size-5); err != nil {
		t.Fatal(err)
	}
	recovered := openTopic(t, filestore.NewOS(dir), params)
	if got := fmt.Sprint(payloadsOf(t, recovered)); got != "[a]" {
		t.Fatalf("after recovery the log holds %s, want the torn write gone whole", got)
	}
	retry := mustProduce(t, recovered, producerA, 2, "b", "c", "d")
	if retry.Duplicate || retry.First != 1 {
		t.Fatalf("the retry got %+v, want it stored at 1", retry)
	}
	if got := fmt.Sprint(payloadsOf(t, recovered)); got != "[a b c d]" {
		t.Errorf("the log holds %s", got)
	}
}

// A write without a producer keeps the old rule: what survives of it is any prefix.
func TestProducer_aTornWriteWithoutAProducerKeepsItsPrefix(t *testing.T) {
	dir := t.TempDir()
	params := Params{MaxFrameEntries: 1}
	topic := openTopic(t, filestore.NewOS(dir), params)
	entries := [][]byte{[]byte("x"), []byte("y"), []byte("z")}
	if err := topic.Write(&entries); err != nil {
		t.Fatal(err)
	}
	topic.Close()
	store := filestore.NewOS(dir)
	blocks, _ := store.List("t", driven.Log)
	if err := store.Truncate(driven.LogRef("t", 0), blocks[0].Size-5); err != nil {
		t.Fatal(err)
	}
	recovered := openTopic(t, filestore.NewOS(dir), params)
	if got := fmt.Sprint(payloadsOf(t, recovered)); got != "[x y]" {
		t.Errorf("after recovery the log holds %s, want the prefix", got)
	}
}

// Frames with and without a producer share a block, and read and reload as one log.
func TestProducer_versionOneAndTwoFramesShareABlock(t *testing.T) {
	dir := t.TempDir()
	topic := openTopic(t, filestore.NewOS(dir), Params{})
	if err := writeOne(topic, "v1"); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, topic, producerA, 1, "v2")
	if err := writeOne(topic, "v1 again"); err != nil {
		t.Fatal(err)
	}
	topic.Close()
	writes, err := logfmt.ScanProducers(filestore.NewOS(dir), driven.LogRef("t", 0))
	if err != nil || len(writes) != 1 || writes[0].First != 1 {
		t.Fatalf("the block's producer writes: %v, %v", writes, err)
	}
	reopened := openTopic(t, filestore.NewOS(dir), Params{})
	if got := fmt.Sprint(payloadsOf(t, reopened)); got != "[v1 v2 v1 again]" {
		t.Errorf("the log holds %s", got)
	}
	got, err := readFrom(reopened, domain.ReadLogParams{From: 2, BatchSize: 10})
	if err != nil || len(got) != 1 || string(got[0].Entry) != "v1 again" {
		t.Errorf("a read from past the v2 frame got %v, %v", got, err)
	}
}

// In strict mode "already written" means durable: a retry that arrives while the original is
// still waiting for its flush does not answer before the flush has.
func TestProducer_aStrictDuplicateWaitsForTheOriginalsFlush(t *testing.T) {
	store := newSyncGate()
	topic := openTopic(t, store, Params{})
	release := store.holdSyncs()
	original := make(chan error, 1)
	go func() { _, err := produce(t, topic, producerA, 1, "a"); original <- err }()
	eventually(t, "the original to be appended", func() bool { return nextOffsetOf(topic) == 1 })
	retried := make(chan domain.WriteResult, 1)
	go func() { result, _ := produce(t, topic, producerA, 1, "a"); retried <- result }()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-retried:
		t.Fatal("the duplicate was answered before the original was durable")
	case <-original:
		t.Fatal("the original returned before its flush")
	default:
	}
	close(release)
	if err := <-original; err != nil {
		t.Fatal(err)
	}
	if result := <-retried; !result.Duplicate {
		t.Errorf("the retry got %+v", result)
	}
}

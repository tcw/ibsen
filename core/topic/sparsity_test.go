package topic

import (
	"fmt"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
	"github.com/tcw/ibsen/core/port/driven"
)

// writeAndIndex writes count entries from an offset, one to a write, and leaves the index
// complete. A write starts indexing in the background and skips it when one is already
// running, so the last entries may not be indexed until this asks once more.
//
// One entry to a write is one entry to a frame, which is what makes the spacing below a
// statement about the sparsity rather than about how a client happened to batch: a pair
// points at the start of a frame and never inside one, so entries sharing a frame share a
// pair whatever sparsity was asked for.
func writeAndIndex(t *testing.T, topic *Topic, from, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		writeEntries(t, topic, from+i, 1)
	}
	if _, err := topic.UpdateIndex(); err != nil {
		t.Fatal(err)
	}
}

func indexPairCount(t *testing.T, topic *Topic, block domain.IndexBlock) int {
	t.Helper()
	bytes := blockBytes(t, topic.Store, topic.indexRef(block))
	if len(bytes)%indexPairSize != 0 {
		t.Fatalf("index block holds %d bytes, not a whole number of %d-byte pairs", len(bytes), indexPairSize)
	}
	return len(bytes) / indexPairSize
}

// The sparsity a caller chooses is the spacing the index actually gets.
func TestChosenSparsityIsWhatGetsIndexed(t *testing.T) {
	const entries = 100
	for _, tc := range []struct {
		sparsity  uint32
		wantPairs int
	}{
		{1, 100}, // every entry
		{10, 10}, // 0, 10, ... 90
		{25, 4},  // 0, 25, 50, 75
		{200, 1}, // only offset 0
	} {
		t.Run(fmt.Sprintf("sparsity=%d", tc.sparsity), func(t *testing.T) {
			topic := NewLogTopic(Params{
				Store: memstore.New(), TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: tc.sparsity,
			})
			if err := topic.LoadOrCreate(); err != nil {
				t.Fatal(err)
			}
			writeAndIndex(t, topic, 0, entries)

			if len(topic.IndexBlockList) != 1 {
				t.Fatalf("expected one index block, got %v", topic.IndexBlockList)
			}
			if got := indexPairCount(t, topic, topic.IndexBlockList[0]); got != tc.wantPairs {
				t.Errorf("index holds %d pairs for %d entries at sparsity %d, want %d",
					got, entries, tc.sparsity, tc.wantPairs)
			}
		})
	}
}

func TestZeroSparsityMeansTheDefault(t *testing.T) {
	topic := NewLogTopic(Params{Store: memstore.New(), TopicName: "t", MaxBlockSize: 1 << 20})
	if topic.IndexSparsity != DefaultIndexSparsity {
		t.Errorf("sparsity is %d for a caller that chose none, want the default %d",
			topic.IndexSparsity, DefaultIndexSparsity)
	}
}

// Sparsity changes how far a read scans, never what it finds.
func TestEveryOffsetIsReadableAtAnySparsity(t *testing.T) {
	const entries = 200
	for _, sparsity := range []uint32{1, 3, 10, 97, 1000} {
		t.Run(fmt.Sprintf("sparsity=%d", sparsity), func(t *testing.T) {
			topic := NewLogTopic(Params{
				Store: memstore.New(), TopicName: "t", MaxBlockSize: 900, IndexSparsity: sparsity,
			})
			if err := topic.LoadOrCreate(); err != nil {
				t.Fatal(err)
			}
			writeAndIndex(t, topic, 0, entries)

			assertReadsFromEveryOffset(t, topic, entries)
		})
	}
}

// Changing sparsity between runs leaves a block indexed at two densities. The pairs already
// written stay valid, so every offset is still readable.
func TestSparsityMayChangeBetweenRuns(t *testing.T) {
	store := memstore.New()
	const firstRun = 120
	const secondRun = 120

	dense := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: 5})
	if err := dense.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeAndIndex(t, dense, 0, firstRun)

	sparse := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: 50})
	if err := sparse.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeAndIndex(t, sparse, firstRun, secondRun)

	// the two densities have to leave one sorted index, or the binary search over it breaks
	pairs := index.NewIndex(blockBytes(t, store, driven.IndexRef("t", sparse.IndexBlockList[0])))
	for i := 1; i < pairs.Size(); i++ {
		if pairs.IndexOffsets[i-1].Offset >= pairs.IndexOffsets[i].Offset {
			t.Fatalf("index is not sorted at pair %d: %v then %v",
				i, pairs.IndexOffsets[i-1], pairs.IndexOffsets[i])
		}
	}

	assertReadsFromEveryOffset(t, sparse, firstRun+secondRun)
}

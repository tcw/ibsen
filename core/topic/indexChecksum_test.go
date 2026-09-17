package topic

import (
	"encoding/binary"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
	"github.com/tcw/ibsen/core/port/driven"
)

// replaceIndexBlock overwrites a topic's index block with exactly these bytes.
func replaceIndexBlock(t *testing.T, store driven.BlockStore, ref driven.BlockRef, bytes []byte) {
	t.Helper()
	if err := store.Truncate(ref, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ref, bytes); err != nil {
		t.Fatal(err)
	}
}

func newIndexedTopic(t *testing.T, store driven.BlockStore, entries int) *Topic {
	t.Helper()
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: 10})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeAndIndex(t, topic, 0, entries)
	return topic
}

// A checksum is only worth having if a bad pair cannot steer a read. A corrupt pair points at
// some byte that is not an entry boundary, so without the checksum a read would scan from
// there and find rubbish.
func TestACorruptedIndexPairIsDroppedAndRebuilt(t *testing.T) {
	const entries = 100
	store := memstore.New()
	topic := newIndexedTopic(t, store, entries)
	ref := topic.indexRef(topic.IndexBlockList[0])

	before := blockBytes(t, store, ref)
	if len(before) != 10*index.PairSize {
		t.Fatalf("index holds %d bytes, want 10 pairs", len(before))
	}
	corrupt := append([]byte(nil), before...)
	corrupt[5*index.PairSize+9] ^= 0xff // the sixth pair's byte offset
	replaceIndexBlock(t, store, ref, corrupt)

	reloaded := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: 10})
	if err := reloaded.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.UpdateIndex(); err != nil {
		t.Fatal(err)
	}

	// the corrupt pair and everything after it was dropped and written again from the log
	after := blockBytes(t, store, ref)
	if len(after) != len(before) {
		t.Fatalf("index holds %d bytes after the rebuild, want %d", len(after), len(before))
	}
	if string(after) != string(before) {
		t.Error("the rebuilt index does not match what a clean scan of the log produces")
	}
	assertReadsFromEveryOffset(t, reloaded, entries)
}

// Index blocks written before pairs carried a checksum are rebuilt rather than misread.
func TestAnIndexWrittenWithoutChecksumsIsRebuilt(t *testing.T) {
	const entries = 60
	store := memstore.New()
	topic := newIndexedTopic(t, store, entries)
	ref := topic.indexRef(topic.IndexBlockList[0])
	want := blockBytes(t, store, ref)

	// the old format: bare (offset, byteOffset) pairs, carrying the same real positions
	var old []byte
	for _, pair := range index.NewIndex(want).IndexOffsets {
		var raw [16]byte
		binary.LittleEndian.PutUint64(raw[:8], uint64(pair.Offset))
		binary.LittleEndian.PutUint64(raw[8:], uint64(pair.ByteOffset))
		old = append(old, raw[:]...)
	}
	replaceIndexBlock(t, store, ref, old)

	reloaded := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, IndexSparsity: 10})
	if err := reloaded.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.UpdateIndex(); err != nil {
		t.Fatal(err)
	}

	if got := blockBytes(t, store, ref); string(got) != string(want) {
		t.Errorf("index is %d bytes after reloading an old one, want the %d a clean scan gives",
			len(got), len(want))
	}
	assertReadsFromEveryOffset(t, reloaded, entries)
}

// Every pair a read relies on verifies, so a read that uses the index lands on a real entry.
func TestReadsStillUseTheIndexAfterChecksumming(t *testing.T) {
	const entries = 100
	store := memstore.New()
	topic := newIndexedTopic(t, store, entries)

	byteOffset, scanned, err := topic.findByteOffsetInLogBlock(domain.Offset(95))
	if err != nil {
		t.Fatal(err)
	}
	if byteOffset == 0 {
		t.Fatal("the index was not used: the scan started at the beginning of the block")
	}
	// offset 90 is indexed, so reaching 95 is five entries of scanning, not ninety-five
	if scanned > 10 {
		t.Errorf("scanned %d entries to reach offset 95, so the index was barely used", scanned)
	}
	assertReadsFromEveryOffset(t, topic, entries)
}

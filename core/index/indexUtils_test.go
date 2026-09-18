package index

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
)

// frame builds one frame holding count entries from firstOffset, which is what a log block
// is made of. The index points at frames, so a test that wants a given number of pairs has
// to say how the entries are grouped into them.
func frame(t *testing.T, firstOffset domain.Offset, count int) []byte {
	t.Helper()
	var entries []byte
	for i := 0; i < count; i++ {
		offset := firstOffset + domain.Offset(i)
		entries = append(entries, domain.CreateByteEntry([]byte("dummy"+strconv.Itoa(int(offset))), offset)...)
	}
	encoded, err := logfmt.EncodeFrame(driven.NoCodec{}, firstOffset, count, entries)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// singleEntryFrames is a block where every entry has a frame of its own, so a pair per frame
// is a pair per entry.
func singleEntryFrames(t *testing.T, entries int) []byte {
	t.Helper()
	var log []byte
	for i := 0; i < entries; i++ {
		log = append(log, frame(t, domain.Offset(i), 1)...)
	}
	return log
}

func TestCreateIndex(t *testing.T) {
	indexBytes, _, err := CreateBinaryIndexFromLog(bytes.NewReader(singleEntryFrames(t, 10)), 0, 1)
	assert.Nil(t, err)
	index := NewIndex(indexBytes)
	// every frame is indexed when oneEntryForEvery is 1, including the block's first
	assert.Equal(t, 10, index.Size())
}

// A sparsity of zero would reach "offset % 0" and panic, so it is refused at the boundary
// rather than left to the modulo.
func TestCreateBinaryIndexFromLogRefusesZeroSparsity(t *testing.T) {
	log := bytes.NewReader(frame(t, 0, 1))

	pairs, byteOffset, err := CreateBinaryIndexFromLog(log, 0, 0)

	if !errors.Is(err, ErrInvalidSparsity) {
		t.Fatalf("got %v, want ErrInvalidSparsity", err)
	}
	if pairs != nil {
		t.Errorf("got %v pairs alongside the error, want none", pairs)
	}
	if byteOffset != 0 {
		t.Errorf("byte offset moved to %d on a refused call", byteOffset)
	}
}

// Sparsity 1 indexes every frame, which is the densest an index gets.
func TestSparsityOneIndexesEveryFrame(t *testing.T) {
	const entries = 5

	pairs, _, err := CreateBinaryIndexFromLog(bytes.NewReader(singleEntryFrames(t, entries)), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(pairs) / PairSize; got != entries {
		t.Errorf("indexed %d of %d frames at sparsity 1", got, entries)
	}
}

// A pair points at the start of a frame and never inside one, so a frame holding several
// entries earns exactly one pair however many of its offsets the sparsity would have picked.
func TestAFrameGetsOnePairHoweverManyEntriesItHolds(t *testing.T) {
	log := frame(t, 0, 25)

	pairs, endByteOffset, err := CreateBinaryIndexFromLog(bytes.NewReader(log), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(pairs) / PairSize; got != 1 {
		t.Fatalf("a frame of 25 entries got %d pairs at sparsity 1, want 1", got)
	}
	pair := NewIndex(pairs).IndexOffsets[0]
	if pair.Offset != 0 || pair.ByteOffset != 0 {
		t.Errorf("pair points at %d -> %d, want the frame start 0 -> 0", pair.Offset, pair.ByteOffset)
	}
	if endByteOffset != int64(len(log)) {
		t.Errorf("scan ended at %d, want the end of the block %d", endByteOffset, len(log))
	}
}

// A frame is indexed when it covers an offset the sparsity picks, so the pairs stay roughly
// one sparsity apart whatever the entries are grouped into. A frame that covers none of them
// is skipped.
func TestFramesAreIndexedWhenTheyCoverAMultipleOfTheSparsity(t *testing.T) {
	// offsets 0..3, 4..7, 8..11, 12..15: at sparsity 10 only the frames covering 0 and 10
	// are worth a pair
	var log []byte
	for first := 0; first < 16; first += 4 {
		log = append(log, frame(t, domain.Offset(first), 4)...)
	}

	pairs, _, err := CreateBinaryIndexFromLog(bytes.NewReader(log), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	index := NewIndex(pairs)
	if index.Size() != 2 {
		t.Fatalf("got %d pairs, want 2: the frames covering offsets 0 and 10", index.Size())
	}
	if got := index.IndexOffsets[0].Offset; got != 0 {
		t.Errorf("first pair is at offset %d, want the frame starting at 0", got)
	}
	if got := index.IndexOffsets[1].Offset; got != 8 {
		t.Errorf("second pair is at offset %d, want the frame 8..11, which covers 10", got)
	}
}

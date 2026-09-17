package index

import (
	"encoding/binary"
	"testing"

	"github.com/tcw/ibsen/core/domain"
)

func encodedPairs(t *testing.T, pairs ...domain.OffsetFilePtr) []byte {
	t.Helper()
	var encoded []byte
	for _, pair := range pairs {
		encoded = AppendPair(encoded, pair)
	}
	if len(encoded) != len(pairs)*PairSize {
		t.Fatalf("%d pairs encoded to %d bytes, want %d", len(pairs), len(encoded), len(pairs)*PairSize)
	}
	return encoded
}

func TestPairRoundTrip(t *testing.T) {
	for _, pair := range []domain.OffsetFilePtr{
		{Offset: 0, ByteOffset: 0},
		{Offset: 1, ByteOffset: 37},
		{Offset: 1 << 40, ByteOffset: 1 << 42},
		{Offset: ^domain.Offset(0), ByteOffset: 1<<63 - 1},
	} {
		got, ok := decodePair(AppendPair(nil, pair))
		if !ok {
			t.Fatalf("%+v did not survive its own checksum", pair)
		}
		if got != pair {
			t.Errorf("round trip gave %+v, want %+v", got, pair)
		}
	}
}

// A flipped bit anywhere in a pair has to be caught, or a read would scan from a byte offset
// that is not an entry boundary.
func TestEveryByteOfAPairIsCovered(t *testing.T) {
	pair := domain.OffsetFilePtr{Offset: 90, ByteOffset: 900}
	encoded := AppendPair(nil, pair)
	for i := range encoded {
		corrupt := append([]byte(nil), encoded...)
		corrupt[i] ^= 0x01
		if _, ok := decodePair(corrupt); ok {
			t.Errorf("a flipped bit in byte %d of a pair went unnoticed", i)
		}
	}
}

// Parsing stops at the first bad pair, so what comes back is a prefix the caller can keep
// and truncate to.
func TestParsingStopsAtACorruptPair(t *testing.T) {
	encoded := encodedPairs(t,
		domain.OffsetFilePtr{Offset: 0, ByteOffset: 0},
		domain.OffsetFilePtr{Offset: 10, ByteOffset: 100},
		domain.OffsetFilePtr{Offset: 20, ByteOffset: 200},
	)
	encoded[PairSize+5] ^= 0xff // second pair

	idx := NewIndex(encoded)

	if idx.Size() != 1 {
		t.Fatalf("kept %d pairs, want the one before the corrupt pair", idx.Size())
	}
	if idx.IndexOffsets[0].Offset != 0 {
		t.Errorf("kept %+v, want the first pair", idx.IndexOffsets[0])
	}
}

func TestATornTrailingPairIsDropped(t *testing.T) {
	encoded := encodedPairs(t,
		domain.OffsetFilePtr{Offset: 0, ByteOffset: 0},
		domain.OffsetFilePtr{Offset: 10, ByteOffset: 100},
	)

	for cut := 1; cut < PairSize; cut++ {
		idx := NewIndex(encoded[:len(encoded)-cut])
		if idx.Size() != 1 {
			t.Errorf("a pair missing its last %d bytes left %d pairs, want 1", cut, idx.Size())
		}
	}
}

// Index blocks used to be bare 16-byte pairs. One of those fails at its first pair, so the
// block is rebuilt from the log rather than read as nonsense.
func TestAnIndexWrittenWithoutChecksumsIsRejected(t *testing.T) {
	var old []byte
	for _, pair := range []struct{ offset, byteOffset uint64 }{{0, 0}, {10, 100}, {20, 200}} {
		var raw [16]byte
		binary.LittleEndian.PutUint64(raw[:8], pair.offset)
		binary.LittleEndian.PutUint64(raw[8:], pair.byteOffset)
		old = append(old, raw[:]...)
	}

	if idx := NewIndex(old); idx.Size() != 0 {
		t.Errorf("an unchecksummed index parsed as %d pairs: %s", idx.Size(), idx.ToString())
	}
}

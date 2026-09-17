package index

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/tcw/ibsen/core/domain"
)

// PairSize is the bytes one pair takes in an index block. A pair on disk is
// crc32c(4) | offset uint64 LE (8) | byteOffset uint64 LE (8), where the checksum covers the
// sixteen bytes after it. That is the shape a log entry already has, so an index block is
// read the same way: a pair either verifies or it is not there.
const PairSize = 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// AppendPair encodes pair onto dst and returns the result, the way append does.
func AppendPair(dst []byte, pair domain.OffsetFilePtr) []byte {
	var encoded [PairSize]byte
	body := encoded[4:]
	binary.LittleEndian.PutUint64(body[:8], uint64(pair.Offset))
	binary.LittleEndian.PutUint64(body[8:], uint64(pair.ByteOffset))
	binary.LittleEndian.PutUint32(encoded[:4], crc32.Checksum(body, crcTable))
	return append(dst, encoded[:]...)
}

// decodePair reads one pair, reporting false when it is short or fails its checksum.
func decodePair(src []byte) (domain.OffsetFilePtr, bool) {
	if len(src) < PairSize {
		return domain.OffsetFilePtr{}, false
	}
	body := src[4:PairSize]
	if binary.LittleEndian.Uint32(src[:4]) != crc32.Checksum(body, crcTable) {
		return domain.OffsetFilePtr{}, false
	}
	return domain.OffsetFilePtr{
		Offset:     domain.Offset(binary.LittleEndian.Uint64(body[:8])),
		ByteOffset: int64(binary.LittleEndian.Uint64(body[8:])),
	}, true
}

type Index struct {
	IndexOffsets []domain.OffsetFilePtr
}

// NewIndex parses the pairs an index block holds, stopping at the first one that is torn or
// fails its checksum. What comes back is the longest good prefix, which is what the caller
// keeps: the bytes after it are truncated away and rebuilt from the log, since the index
// says nothing the log does not.
//
// A block written before pairs carried a checksum fails at its first pair and is rebuilt
// whole, so the format change needs no migration.
func NewIndex(bytes []byte) *Index {
	index := Index{IndexOffsets: make([]domain.OffsetFilePtr, 0)}
	for i := 0; i+PairSize <= len(bytes); i += PairSize {
		pair, ok := decodePair(bytes[i : i+PairSize])
		if !ok {
			break
		}
		index.add(pair)
	}
	return &index
}

func (idx *Index) Size() int {
	return len(idx.IndexOffsets)
}

func (idx *Index) IsEmpty() bool {
	return len(idx.IndexOffsets) == 0
}

func (idx *Index) Head() domain.OffsetFilePtr {
	if idx.IsEmpty() {
		return domain.OffsetFilePtr{}
	}
	return idx.IndexOffsets[len(idx.IndexOffsets)-1]
}

func (idx *Index) ToString() string {
	indexToString := fmt.Sprintf("log offset -> byte offset\n")
	for _, offset := range idx.IndexOffsets {
		indexToString = indexToString + fmt.Sprintf("%d -> %d\n", offset.Offset, offset.ByteOffset)
	}
	return indexToString
}

// FindNearestByteOffset returns the last pair at or before offset, which is where a read
// starts scanning for it. A zero pair means the index holds nothing at or before offset, so
// the scan starts at the beginning of the block.
//
// The pairs are appended in the order the log was scanned, so they are sorted by offset and
// the answer is one before the first pair past offset. A block holds at most
// MaxBlockSize/indexSparsity pairs, so this is a binary search over a sorted slice rather
// than the scan back from the end it replaced.
func (idx *Index) FindNearestByteOffset(offset domain.Offset) domain.OffsetFilePtr {
	past := sort.Search(len(idx.IndexOffsets), func(i int) bool {
		return idx.IndexOffsets[i].Offset > offset
	})
	if past == 0 {
		return domain.OffsetFilePtr{}
	}
	return idx.IndexOffsets[past-1]
}

func (idx *Index) add(pair domain.OffsetFilePtr) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair)
}

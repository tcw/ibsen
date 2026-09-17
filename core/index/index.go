package index

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tcw/ibsen/core/domain"
)

type Index struct {
	IndexOffsets []domain.OffsetFilePtr
}

func NewIndex(bytes []byte) *Index {
	batchSize := 16
	index := Index{IndexOffsets: make([]domain.OffsetFilePtr, 0)}
	for i := 0; i < len(bytes); i += batchSize {
		end := i + batchSize
		if end > len(bytes) {
			return &index
		}
		index.add(domain.OffsetFilePtr{
			Offset:     domain.Offset(binary.LittleEndian.Uint64(bytes[i : end-8])),
			ByteOffset: int64(binary.LittleEndian.Uint64(bytes[i+8 : end])),
		})
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

func (idx *Index) addAll(pair []domain.OffsetFilePtr) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair...)
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.IndexOffsets)
}

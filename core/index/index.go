package index

import (
	"encoding/binary"
	"fmt"

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

// Todo: this is linear search, should use range tree for large indices
func (idx *Index) FindNearestByteOffset(offset domain.Offset) domain.OffsetFilePtr {
	for i := len(idx.IndexOffsets) - 1; i >= 0; i-- {
		if offset >= idx.IndexOffsets[i].Offset {
			byteOffset := idx.IndexOffsets[i]
			return byteOffset
		}
	}
	return domain.OffsetFilePtr{}
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

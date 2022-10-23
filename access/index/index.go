package index

import (
	"encoding/binary"
	"fmt"
	"github.com/tcw/ibsen/access/common"
)

type Index struct {
	IndexOffsets []common.OffsetFilePtr
}

func NewIndex(bytes []byte) *Index {
	batchSize := 16
	index := Index{IndexOffsets: make([]common.OffsetFilePtr, 0)}
	for i := 0; i < len(bytes); i += batchSize {
		end := i + batchSize
		if end > len(bytes) {
			return &index
		}
		index.add(common.OffsetFilePtr{
			Offset:     common.Offset(binary.LittleEndian.Uint64(bytes[i : end-8])),
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

func (idx *Index) Head() common.OffsetFilePtr {
	if idx.IsEmpty() {
		return common.OffsetFilePtr{}
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
func (idx *Index) FindNearestByteOffset(offset common.Offset) common.OffsetFilePtr {
	for i := len(idx.IndexOffsets) - 1; i >= 0; i-- {
		if offset >= idx.IndexOffsets[i].Offset {
			byteOffset := idx.IndexOffsets[i]
			return byteOffset
		}
	}
	return common.OffsetFilePtr{}
}

func (idx *Index) add(pair common.OffsetFilePtr) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair)
}

func (idx *Index) addAll(pair []common.OffsetFilePtr) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair...)
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.IndexOffsets)
}

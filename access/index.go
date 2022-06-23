package access

import (
	"fmt"
)

type IndexOffset struct {
	Offset     Offset
	ByteOffset int64
}

type Index struct {
	IndexOffsets []IndexOffset
}

func (idx Index) Size() int {
	return len(idx.IndexOffsets)
}

func (idx Index) IsEmpty() bool {
	return len(idx.IndexOffsets) == 0
}

func (idx Index) Head() IndexOffset {
	if idx.IsEmpty() {
		return IndexOffset{}
	}
	return idx.IndexOffsets[len(idx.IndexOffsets)-1]
}

func (idx Index) ToString() string {
	indexToString := fmt.Sprintf("log offset -> byte offset\n")
	for _, offset := range idx.IndexOffsets {
		indexToString = indexToString + fmt.Sprintf("%d -> %d\n", offset.Offset, offset.ByteOffset)
	}
	return indexToString
}

//Todo: this is linear search, should use range tree for large indices
func (idx Index) findNearestByteOffset(offset Offset) IndexOffset {
	for i := len(idx.IndexOffsets) - 1; i >= 0; i-- {
		if offset >= idx.IndexOffsets[i].Offset {
			byteOffset := idx.IndexOffsets[i]
			return byteOffset
		}
	}
	return IndexOffset{}
}

func (idx *Index) add(pair IndexOffset) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair)
}

func (idx *Index) addAll(pair []IndexOffset) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair...)
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.IndexOffsets)
}

func (ido IndexOffset) IsEmpty() bool {
	return ido.ByteOffset == 0
}

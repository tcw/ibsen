package access

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

func (idx *Index) add(pair IndexOffset) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair)
}

func (idx *Index) addAll(pair []IndexOffset) {
	idx.IndexOffsets = append(idx.IndexOffsets, pair...)
}

//Todo: this is linear search, should use range tree for large indices
func (idx Index) FindNearestByteOffset(offset Offset) int64 {
	for i := len(idx.IndexOffsets) - 1; i >= 0; i-- {
		if offset > idx.IndexOffsets[i].Offset {
			return idx.IndexOffsets[i].ByteOffset
		}
	}
	return 0
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.IndexOffsets)
}

type IndexOffset struct {
	Offset     Offset
	ByteOffset int64
}

func (ido IndexOffset) IsEmpty() bool {
	return ido.ByteOffset == 0
}

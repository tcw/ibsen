package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

type Index struct {
	Offsets []OffsetPair
}

func (idx *Index) add(pair OffsetPair) {
	idx.Offsets = append(idx.Offsets, pair)
}

func (idx *Index) addAll(pair []OffsetPair) {
	idx.Offsets = append(idx.Offsets, pair...)
}

// this is linear search, should use range tree for large indices
func (idx Index) findNearestByteOffset(offset Offset) int64 {
	for i := len(idx.Offsets) - 1; i >= 0; i-- {
		if offset > idx.Offsets[i].Offset {
			return idx.Offsets[i].byteOffset
		}
	}
	return 0
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.Offsets)
}

type OffsetPair struct {
	Offset     Offset
	byteOffset int64
}

func loadStrictlyMonotonicOrderedVarIntIndex(afs *afero.Afero, indexFileName string) (StrictlyMonotonicOrderedVarIntIndex, error) {
	file, err := OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return bytes, nil
}

func fromStrictOrderVarIntIndexToIndex(soi StrictlyMonotonicOrderedVarIntIndex, oneInEvery uint32) (Index, error) {
	var numberPart []byte
	var offset uint64
	var byteOffsetAccumulated int64
	var index = Index{}
	for _, byteValue := range soi {
		numberPart = append(numberPart, byteValue)
		if !isLittleEndianMSBSet(byteValue) {
			byteOffset, n := protowire.ConsumeVarint(numberPart)
			if n < 0 {
				return Index{}, errore.NewWithContext("Vararg returned negative numberPart, indicating a parsing error")
			}
			offset = offset + uint64(oneInEvery)
			byteOffsetAccumulated = byteOffsetAccumulated + int64(byteOffset)
			index.add(OffsetPair{
				Offset:     Offset(offset),
				byteOffset: byteOffsetAccumulated,
			})

			numberPart = make([]byte, 0)
		}
	}
	return index, nil
}

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

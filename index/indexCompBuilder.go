package index

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

func writeToFile(file afero.File, previousPosition storage.OffsetPosition, offsetPositions []storage.OffsetPosition) error {
	var bytes []byte
	lastPosition := previousPosition
	for _, position := range offsetPositions {
		bytes = protowire.AppendVarint(bytes, position.Offset-lastPosition.Offset)
		bytes = protowire.AppendVarint(bytes, position.ByteOffset-lastPosition.ByteOffset)
		lastPosition = storage.OffsetPosition{
			Offset:     position.Offset,
			ByteOffset: position.ByteOffset,
		}
	}
	_, err := file.Write(bytes)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func readFromFile(afs *afero.Afero, indexFileName string) (map[commons.Offset]commons.ByteOffset, error) {
	file, err := commons.OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var byteValue = make([]byte, 1)
	positions := make(map[commons.Offset]commons.ByteOffset)
	reader := bufio.NewReader(file)
	var counter uint64 = 0
	var number []byte
	var offset uint64
	var byteOffset uint64
	for {
		_, err := reader.Read(byteValue)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		number = append(number, byteValue[0])
		if !isLittleEndianMSBSet(byteValue[0]) {
			varint, n := protowire.ConsumeVarint(number)
			if n < 0 {
				return nil, errore.NewWithContext("Vararg returned negative number, indicating a parsing error")
			}
			if counter%2 == 0 {
				offset = offset + varint
			} else {
				byteOffset = byteOffset + varint
				positions[commons.Offset(offset)] = commons.ByteOffset(byteOffset)
			}
			counter = counter + 1
			number = make([]byte, 0)
		}
	}
	return positions, err
}

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

package index

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

func writeToFile(file afero.File, offsetPositions []storage.OffsetPosition) error {
	protowire.AppendVarint()
	return nil
}

type Offset uint64
type ByteOffset uint64

func readFromFile(afs *afero.Afero, indexFileName string) (map[Offset]ByteOffset, error) {
	file, err := storage.OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var byteValue = make([]byte, 1)
	positions := make(map[Offset]ByteOffset)
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
		if isLittleEndianMSBSet(byteValue[0]) {
			varint, n := protowire.ConsumeVarint(number)
			if n < 0 {
				return nil, errore.NewWithContext("Vararg returned negative number, indicating a parsing error")
			}
			if counter%2 == 0 {
				offset = offset + varint
			} else {
				byteOffset = byteOffset + varint
				positions[Offset(offset)] = ByteOffset(byteOffset)
			}
			counter = counter + 1
		} else {
			number = append(number, byteValue[0])
		}
	}
	return positions, err
}

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

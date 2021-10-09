package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

//

func buildIndex(file afero.File, logfileByteOffset int64, oneEntryForEvery uint32) (StrictOrderIndex, error) {

	var index StrictOrderIndex
	var currentByteOffset int64 = -1
	var lastOffset int64 = 0
	var offset uint64 = 1
	var err error
	if logfileByteOffset > 0 {
		currentByteOffset, err = file.Seek(logfileByteOffset, io.SeekStart)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	head := make([]byte, 12)
	for {
		headSize, err := io.ReadFull(reader, head)
		if err == io.EOF {
			return index, nil
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(headSize)

		byteSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(byteSize)
		size := commons.LittleEndianToUint64(bytes)

		entry := make([]byte, size)
		entrySize, err := io.ReadFull(reader, entry)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(entrySize)
		if offset%uint64(oneEntryForEvery) == 0 {
			element := indexElement(lastOffset, currentByteOffset)
			index = append(index, element...)
			lastOffset = currentByteOffset
		}
		offset = offset + 1
	}
}

func indexElement(lastByteOffset int64, currentByteOffset int64) []byte {
	var bytes []byte
	bytes = protowire.AppendVarint(bytes, uint64(currentByteOffset-lastByteOffset))

}

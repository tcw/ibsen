package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

func saveIndex(afs *afero.Afero, indexFileName FileName, index StrictlyMonotonicOrderedVarIntIndex) error {
	file, err := OpenFileForWrite(afs, string(indexFileName))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	_, err = file.Write(index)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func createHeadIndex(afs *afero.Afero, logFile FileName, logfileByteOffset int64, oneEntryForEvery uint32) (Index, int64, error) {
	file, err := OpenFileForRead(afs, string(logFile))
	defer file.Close()
	if err != nil {
		return Index{}, 0, errore.WrapWithContext(err)
	}
	var index = Index{}
	var currentByteOffset int64 = 0
	var offset uint64 = 0
	if logfileByteOffset > 0 {
		currentByteOffset, err = file.Seek(logfileByteOffset, io.SeekStart)
		if err != nil {
			return Index{}, 0, errore.WrapWithContext(err)
		}
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	head := make([]byte, 12)
	for {
		headSize, err := io.ReadFull(reader, head)
		if err == io.EOF {
			return index, currentByteOffset, nil
		}
		if err != nil {
			return Index{}, 0, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(headSize)

		byteSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			return Index{}, 0, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(byteSize)
		size := commons.LittleEndianToUint64(bytes)

		entry := make([]byte, size)
		entrySize, err := io.ReadFull(reader, entry)
		if err != nil {
			return Index{}, 0, errore.WrapWithContext(err)
		}
		currentByteOffset = currentByteOffset + int64(entrySize)
		if offset%uint64(oneEntryForEvery) == 0 {
			index.add(OffsetPair{
				Offset:     Offset(offset),
				byteOffset: currentByteOffset,
			})
		}
		offset = offset + 1
	}
}

func createArchiveIndex(afs *afero.Afero, logFile string, logfileByteOffset int64, oneEntryForEvery uint32) (StrictlyMonotonicOrderedVarIntIndex, error) {

	file, err := OpenFileForRead(afs, logFile)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var index StrictlyMonotonicOrderedVarIntIndex
	var currentByteOffset int64 = 0
	var lastOffset int64 = 0
	var offset uint64 = 0
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
	return protowire.AppendVarint(bytes, uint64(currentByteOffset-lastByteOffset))
}

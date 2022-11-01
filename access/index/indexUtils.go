package index

import (
	"bufio"
	"encoding/binary"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
	"io"
)

func CreateBinaryIndexFromLogFile(afs *afero.Afero, logFileName string, logfileByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	exists, err := afs.Exists(logFileName)
	if err != nil {
		return nil, 0, errore.Wrap(err)
	}
	if !exists {
		return nil, 0, errore.New("NoFile")
	}

	file, err := common.OpenFileForRead(afs, logFileName)
	if err != nil {
		return nil, 0, errore.Wrap(err)
	}
	var index []uint64
	var byteOffset int64 = 0
	if logfileByteOffset > 0 {
		byteOffset, err = file.Seek(logfileByteOffset, io.SeekStart)
		if err != nil {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return nil, byteOffset, errore.Wrap(err)
		}
	}
	isFirst := true
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	bytesCrc := make([]byte, 4)
	for {
		crcSize, err := io.ReadFull(reader, bytesCrc)
		if err == io.EOF {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return common.Uint64ArrayToBytes(index), byteOffset, nil
		}
		if err != nil {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return nil, byteOffset, errore.Wrap(err)
		}
		byteSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return nil, byteOffset, errore.Wrap(err)
		}
		size := binary.LittleEndian.Uint32(bytes)
		entry := make([]byte, size)
		entrySize, err := io.ReadFull(reader, entry)
		if err != nil {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return nil, byteOffset, errore.Wrap(err)
		}
		offsetSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			ioErr := file.Close()
			if ioErr != nil {
				return nil, 0, errore.WrapError(ioErr, err)
			}
			return nil, byteOffset, errore.Wrap(err)
		}
		offset := binary.LittleEndian.Uint64(bytes)
		if !isFirst && offset%uint64(oneEntryForEvery) == 0 {
			index = append(index, offset)
			index = append(index, uint64(byteOffset))
		}
		isFirst = false
		byteOffset = byteOffset + int64(offsetSize+crcSize+byteSize+entrySize)
	}
}

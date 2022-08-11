package access

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"io"
)

func MarshallIndex(soi []byte) (Index, error) {
	if len(soi) == 0 {
		return Index{}, errore.New("NoBytesInIndex")
	}

	var numberPart []byte
	var offset uint64
	isOffset := true
	var index = Index{
		IndexOffsets: nil,
	}
	for _, byteValue := range soi {
		numberPart = append(numberPart, byteValue)
		if !isLittleEndianMSBSet(byteValue) {
			value, n := proto.DecodeVarint(numberPart)
			if n < 0 {
				return Index{}, errore.NewF("Vararg returned negative numberPart, indicating a parsing error")
			}
			if isOffset {
				offset = value
				isOffset = false
			} else {
				index.add(IndexOffset{
					Offset:     Offset(offset),
					ByteOffset: int64(value),
				})
				isOffset = true
			}
			numberPart = make([]byte, 0)
		}
	}
	return index, nil
}

func CreateIndex(afs *afero.Afero, logFileName string, logfileByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	exists, err := afs.Exists(logFileName)
	if err != nil {
		return nil, 0, errore.Wrap(err)
	}
	if !exists {
		return nil, 0, errore.New("NoFile")
	}

	file, err := OpenFileForRead(afs, logFileName)
	if err != nil {
		return nil, 0, errore.Wrap(err)
	}
	var index []byte
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
			return index, byteOffset, nil
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
		size := littleEndianToUint32(bytes)
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
		offset := littleEndianToUint64(bytes)
		if !isFirst && offset%uint64(oneEntryForEvery) == 0 {
			offsetVarInt := toVarInt(int64(offset))
			byteOffsetVarInt := toVarInt(byteOffset)
			index = append(index, offsetVarInt...)
			index = append(index, byteOffsetVarInt...)
		}
		isFirst = false
		byteOffset = byteOffset + int64(offsetSize+crcSize+byteSize+entrySize)
	}
}

func toVarInt(byteOffset int64) []byte {
	return proto.EncodeVarint(uint64(byteOffset))
}

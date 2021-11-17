package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
	"math"
	"path"
	"strings"
)

type LogIndexAccess interface {
	WriteFromOffset(logfile FileName, logfileByteOffset int64) (Offset, error)
	WriteFile(logfile FileName) (Offset, error)
	Read(logfile FileName) (Index, error)
	ReadTopicIndexBlocks(topic Topic) (Blocks, error)
}

var _ LogIndexAccess = ReadWriteLogIndexAccess{}

type ReadWriteLogIndexAccess struct {
	Afs          *afero.Afero
	RootPath     string
	IndexDensity float64
}

func (r ReadWriteLogIndexAccess) WriteFromOffset(logfile FileName, logfileByteOffset int64) (Offset, error) {
	index, err := createIndex(r.Afs, logfile, logfileByteOffset, densityToOneInEvery(r.IndexDensity))
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	err = saveIndex(r.Afs, logFileToIndexFile(logfile), index)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return 0, nil
}

func (r ReadWriteLogIndexAccess) WriteFile(logfile FileName) (Offset, error) {
	index, err := createIndex(r.Afs, logfile, 0, densityToOneInEvery(r.IndexDensity))
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	err = saveIndex(r.Afs, logFileToIndexFile(logfile), index)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return 0, nil
}

func (r ReadWriteLogIndexAccess) Read(logfile FileName) (Index, error) {
	index, err := loadIndex(r.Afs, string(logFileToIndexFile(logfile)))
	if err != nil {
		return Index{}, errore.WrapWithContext(err)
	}
	return toMarshalledIndex(index)
}

func (r ReadWriteLogIndexAccess) ReadTopicIndexBlocks(topic Topic) (Blocks, error) {
	panic("implement me")
}

func loadIndex(afs *afero.Afero, indexFileName string) ([]byte, error) {
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

func toMarshalledIndex(soi []byte) (Index, error) {
	var numberPart []byte
	var offset uint64
	var index = Index{}
	isOffset := true
	for _, byteValue := range soi {
		numberPart = append(numberPart, byteValue)
		if !isLittleEndianMSBSet(byteValue) {
			value, n := protowire.ConsumeVarint(numberPart)
			if n < 0 {
				return Index{}, errore.NewWithContext("Vararg returned negative numberPart, indicating a parsing error")
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

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

func densityToOneInEvery(density float64) uint32 {
	return uint32(math.Floor(1.0 / density))
}

func logFileToIndexFile(logfile FileName) FileName {
	ext := path.Ext(string(logfile))
	return FileName(strings.Replace(string(logfile), ext, ".idx", 1))
}

func saveIndex(afs *afero.Afero, indexFileName FileName, index []byte) error {
	file, err := openFileForWrite(afs, string(indexFileName))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	_, err = file.Write(index)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func createIndex(afs *afero.Afero, logFile FileName, logfileByteOffset int64, oneEntryForEvery uint32) ([]byte, error) {
	file, err := OpenFileForRead(afs, string(logFile))
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var index []byte
	var byteOffset int64 = 0
	if logfileByteOffset > 0 {
		byteOffset, err = file.Seek(logfileByteOffset, io.SeekStart)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	bytesCrc := make([]byte, 4)
	isFirst := true
	for {
		offsetSize, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return index, nil
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		offset := littleEndianToUint64(bytes)
		crcSize, err := io.ReadFull(reader, bytesCrc)
		if err == io.EOF {
			return index, nil
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		byteSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)

		entry := make([]byte, size)
		entrySize, err := io.ReadFull(reader, entry)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		if !isFirst && offset%uint64(oneEntryForEvery) == 0 {
			offsetVarInt := toVarInt(int64(offset))
			byteOffsetVarInt := toVarInt(byteOffset)
			index = append(index, offsetVarInt...)
			index = append(index, byteOffsetVarInt...)
		}
		byteOffset = byteOffset + int64(offsetSize+crcSize+byteSize+entrySize)
		isFirst = false
	}
}

func toVarInt(byteOffset int64) []byte {
	var bytes []byte
	return protowire.AppendVarint(bytes, uint64(byteOffset))
}

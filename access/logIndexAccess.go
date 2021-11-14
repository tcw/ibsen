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
	return toMarshalledIndex(index, densityToOneInEvery(r.IndexDensity))
}

func (r ReadWriteLogIndexAccess) ReadTopicIndexBlocks(topic Topic) (Blocks, error) {
	panic("implement me")
}

func loadIndex(afs *afero.Afero, indexFileName string) ([]byte, error) {
	file, err := openFileForRead(afs, indexFileName)
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

func toMarshalledIndex(soi []byte, oneInEvery uint32) (Index, error) {
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
			index.add(IndexOffset{
				Offset:     Offset(offset),
				ByteOffset: byteOffsetAccumulated,
			})

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

//Todo: add header that contains density
func createIndex(afs *afero.Afero, logFile FileName, logfileByteOffset int64, oneEntryForEvery uint32) ([]byte, error) {
	file, err := openFileForRead(afs, string(logFile))
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var index []byte
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
		size := littleEndianToUint64(bytes)

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

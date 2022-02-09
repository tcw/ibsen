package access

import (
	"bufio"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"io"
	"math"
	"path"
	"strings"
)

type LogIndexAccess interface {
	Write(logfile FileName, logfileByteOffset int64) (Offset, error)
	Read(indexLogFile FileName) (Index, error)
	ReadTopicIndexBlocks(topic Topic) (Blocks, error)
}

type IndexType uint16
type CompressionType uint16

var NoFile error = errors.New("no such file")
var NoBytesInIndex = errors.New("no bytes in index")

var _ LogIndexAccess = ReadWriteLogIndexAccess{}

type ReadWriteLogIndexAccess struct {
	Afs          *afero.Afero
	RootPath     string
	IndexDensity float64
}

func (r ReadWriteLogIndexAccess) Write(logfile FileName, logfileByteOffset int64) (Offset, error) {
	index, err := createIndex(r.Afs, logfile, logfileByteOffset, densityToOneInEvery(r.IndexDensity))
	if err == NoFile {
		return 0, err
	}
	if index == nil {
		return 0, nil
	}
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	err = saveIndex(r.Afs, logFileToIndexFile(logfile), index)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return 0, nil
}

func (r ReadWriteLogIndexAccess) Read(indexLogfile FileName) (Index, error) {
	exists, err := r.Afs.Exists(string(indexLogfile))
	if err != nil {
		return Index{}, err
	}
	if !exists {
		return Index{}, nil
	}
	size, err := FileSize(r.Afs, string(indexLogfile))
	if err != nil {
		return Index{}, err
	}
	if size == 0 {
		return Index{}, nil
	}
	index, err := LoadIndex(r.Afs, string(indexLogfile))
	if err != nil {
		return Index{}, errore.WrapWithContext(err)
	}
	marshalledIndex, err := MarshallIndex(index)
	if err != nil {
		return Index{}, errore.WrapWithContextAndMessage(err, "reading index file: %s", indexLogfile)
	}
	return marshalledIndex, err
}

func (r ReadWriteLogIndexAccess) ReadTopicIndexBlocks(topic Topic) (Blocks, error) {
	directory, err := listFilesInDirectory(r.Afs, r.RootPath+Sep+string(topic), ".idx")
	if err != nil {
		return Blocks{}, errore.WrapWithContext(err)
	}
	blocks, err := filesToBlocks(directory)
	domainBlocks := Blocks{BlockList: blocks}
	domainBlocks.Sort()
	return domainBlocks, nil
}

func LoadIndex(afs *afero.Afero, indexFileName string) ([]byte, error) {
	file, err := OpenFileForRead(afs, indexFileName)
	if err != nil {
		return nil, errore.WrapWithContextAndMessage(err, "opening file: %s", indexFileName)
	}
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

func MarshallIndex(soi []byte) (Index, error) {
	if len(soi) == 0 {
		return Index{}, NoBytesInIndex
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
	exists, err := afs.Exists(string(logFile))
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	if !exists {
		return nil, NoFile
	}

	file, err := OpenFileForRead(afs, string(logFile))
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer file.Close()
	var index []byte
	var byteOffset int64 = 0
	if logfileByteOffset > 0 {
		byteOffset, err = file.Seek(logfileByteOffset, io.SeekStart)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
	}
	isFirst := true
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	bytesCrc := make([]byte, 4)
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
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		byteSize, err := io.ReadFull(reader, bytes)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		size := littleEndianToUint32(bytes)

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
		isFirst = false
		byteOffset = byteOffset + int64(offsetSize+crcSize+byteSize+entrySize)
	}
}

// Todo: evaluate if this is needed
// |-- (index type) 2 byte --|-- (compression type) 2 byte --|
//func createIndexHeader(indexHeaderType IndexType, compression CompressionType) []byte {
//	var indexHeader []byte
//	typeBytes := uint16ToLittleEndian(uint16(indexHeaderType))
//	compressionBytes := uint16ToLittleEndian(uint16(compression))
//	indexHeader = append(indexHeader, typeBytes...)
//	indexHeader = append(indexHeader, compressionBytes...)
//	return indexHeader
//}

func toVarInt(byteOffset int64) []byte {
	return proto.EncodeVarint(uint64(byteOffset))
}

const (
	FixedInterval IndexType = iota
	//Next
)

const (
	NoCompression CompressionType = iota
	//StandardZ
	//Snappy
)

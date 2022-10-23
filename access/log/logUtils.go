package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type BlockSizeInBytes uint64
type FileName string
type StrictlyMonotonicVarIntIndex []byte

var NoByteOffsetFound = errors.New("no byte offset found")

func CreateTopicDirectory(afs *afero.Afero, rootPath string, topic string) (bool, error) {
	path := rootPath + common.Sep + topic
	exists, err := afero.Exists(afs, path)
	if err != nil {
		return false, errore.Wrap(err)
	}
	if !exists {
		err = afs.Mkdir(path, 0744)
		if err != nil {
			return false, errore.Wrap(err)
		}
		return true, nil
	}
	return false, nil
}

func ListAllFilesInTopic(afs *afero.Afero, rootPath string, topic string) ([]os.FileInfo, error) {
	dir, err := common.OpenFileForRead(afs, rootPath+common.Sep+topic)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	defer dir.Close()
	return dir.Readdir(0)
}

func LoadTopicBlocks(afs *afero.Afero, rootPath string, topic string) ([]common.LogBlock, []common.IndexBlock, error) {
	filesInTopic, err := ListAllFilesInTopic(afs, rootPath, topic)
	if err != nil {
		return nil, nil, errore.Wrap(err)
	}
	var indexBlocks []common.IndexBlock
	var logBlocks []common.LogBlock
	for _, info := range filesInTopic {
		if info.IsDir() {
			continue
		}
		nameExt := strings.Split(info.Name(), ".")
		fileExtension := filepath.Ext(info.Name())
		parseUint, err := strconv.ParseUint(nameExt[0], 10, 64)
		if err != nil {
			return nil, nil, errore.Wrap(err)
		}
		if fileExtension == ".log" {
			logBlocks = append(logBlocks, common.LogBlock(parseUint))
		}
		if fileExtension == ".idx" {
			indexBlocks = append(indexBlocks, common.IndexBlock(parseUint))
		}
	}
	sort.Slice(indexBlocks, func(i, j int) bool { return indexBlocks[i] < indexBlocks[j] })
	sort.Slice(logBlocks, func(i, j int) bool { return logBlocks[i] < logBlocks[j] })
	return logBlocks, indexBlocks, nil
}

func ListAllTopics(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := common.OpenFileForRead(afs, dir)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	defer file.Close()
	names, err := file.Readdirnames(0)
	for _, name := range names {
		isHidden := strings.HasPrefix(name, ".")
		if !isHidden {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func FindByteOffsetFromAndIncludingOffset(afs *afero.Afero, fileName string, startAtByteOffset int64, offset common.Offset) (int64, int, error) {
	scanCount := 0
	if offset == 0 {
		return 0, 0, nil
	}
	file, err := common.OpenFileForRead(afs, fileName)
	if err != nil {
		return 0, scanCount, errore.Wrap(err)
	}
	defer file.Close()

	if startAtByteOffset > 0 {
		_, err = file.Seek(startAtByteOffset, io.SeekStart)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		lastOffset, err := offsetLookBack(file)
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		if lastOffset+1 == offset {
			return startAtByteOffset, 0, nil
		}
	}

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)

	var offsetInFile int64 = math.MaxInt64 - 1
	var byteOffset = startAtByteOffset
	for {
		if common.Offset(offsetInFile+1) == offset {
			return byteOffset, scanCount, nil
		}
		checksumBytes, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			return 0, scanCount, NoByteOffsetFound
		}
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		sizeBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		entrySize := binary.LittleEndian.Uint64(bytes)
		entryBytes := make([]byte, entrySize)
		_, err = io.ReadFull(reader, entryBytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		offsetBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		offsetInFile = int64(binary.LittleEndian.Uint64(bytes))
		byteOffset = byteOffset + int64(offsetBytes) + int64(checksumBytes) + int64(sizeBytes) + int64(entrySize)
		scanCount = scanCount + 1
	}
}

func offsetLookBack(file afero.File) (common.Offset, error) {
	_, err := file.Seek(-8, io.SeekCurrent)
	if err != nil {
		return 0, errore.Wrap(err)
	}
	bytes := make([]byte, 8)
	_, err = io.ReadFull(file, bytes)
	if err != nil {
		return 0, errore.Wrap(err)
	}
	return common.Offset(binary.LittleEndian.Uint64(bytes)), nil
}

func BlockInfo(afs *afero.Afero, blockFileName string) (common.Offset, int64, error) {
	file, err := common.OpenFileForRead(afs, blockFileName)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	seek, err := file.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	fileSize := seek + 8
	bytes := make([]byte, 8)
	_, err = io.ReadFull(file, bytes)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	offset := int64(binary.LittleEndian.Uint64(bytes))
	return common.Offset(offset), fileSize, nil
}

type ReadFileParams struct {
	File            afero.File
	LogChan         chan *[]common.LogEntry
	Wg              *sync.WaitGroup
	BatchSize       uint32
	StartByteOffset int64
	EndOffset       common.Offset
}

type ReadResult struct {
	LastLogOffset common.Offset
	EntriesRead   uint64
}

func (r *ReadResult) Update(result ReadResult) {
	r.LastLogOffset = result.LastLogOffset
	r.EntriesRead = r.EntriesRead + result.EntriesRead
}

func (r *ReadResult) NextOffset() common.Offset {
	return r.LastLogOffset + 1
}

func ReadFile(params ReadFileParams) (ReadResult, error) {
	var currentOffset common.Offset = 0
	var offsetFromLogg common.Offset = 0
	var entriesRead uint64 = 0
	currentBatchInBytes := 0
	log.Debug().
		Str("filename", params.File.Name()).
		Int64("byteOffset", params.StartByteOffset).
		Msg("read file")
	if params.StartByteOffset > 0 {
		_, err := params.File.Seek(params.StartByteOffset, io.SeekStart)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		offsetFromLogg, err = offsetLookBack(params.File)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		currentOffset = offsetFromLogg + 1
	}
	reader := bufio.NewReader(params.File)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	logEntries := make([]common.LogEntry, params.BatchSize)
	slicePointer := 0
	for {
		if currentOffset == params.EndOffset {
			if logEntries != nil && slicePointer > 0 {
				params.Wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				params.LogChan <- &sendingEntries
			}
			return ReadResult{
				LastLogOffset: offsetFromLogg,
				EntriesRead:   entriesRead,
			}, nil
		}
		if (slicePointer != 0 &&
			uint32(slicePointer)%params.BatchSize == 0) ||
			currentBatchInBytes > 10*1024*1024 {
			params.Wg.Add(1)
			sendingEntries := logEntries[:slicePointer]
			logEntryCopy := make([]common.LogEntry, slicePointer)
			copy(logEntryCopy, sendingEntries)
			params.LogChan <- &logEntryCopy
			slicePointer = 0
			currentBatchInBytes = 0
		}
		// Checksum
		_, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			if logEntries != nil && slicePointer > 0 {
				params.Wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				params.LogChan <- &sendingEntries
			}
			return ReadResult{
				LastLogOffset: offsetFromLogg,
				EntriesRead:   entriesRead,
			}, nil
		}
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		checksumValue := binary.LittleEndian.Uint32(bytes)

		// Entry size
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}

		size := binary.LittleEndian.Uint64(bytes)

		// Entry bytes
		entry := make([]byte, size)

		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}

		// offset
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}

		offset := int64(binary.LittleEndian.Uint64(bytes))
		offsetFromLogg = common.Offset(offset)
		if currentOffset == 0 {
			currentOffset = offsetFromLogg
		}
		if currentOffset != offsetFromLogg {
			return ReadResult{}, errore.NewF("read order assertion failed, expected [%d] actual [%d]", currentOffset, offsetFromLogg)
		}
		logEntries[slicePointer] = common.LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		}
		currentBatchInBytes = +int(size)
		entriesRead = entriesRead + 1
		currentOffset = currentOffset + 1
		slicePointer = slicePointer + 1
	}
}

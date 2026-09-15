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

// maxBatchBytes caps the payload bytes collected into one read batch.
const maxBatchBytes = 10 * 1024 * 1024

// maxBatchPrealloc caps the entries preallocated for a batch, since the batch size comes from clients.
const maxBatchPrealloc = 1024

var NoByteOffsetFound = errors.New("no byte offset found")

func CreateTopicDirectory(afs *afero.Afero, rootPath string, topic string) (bool, error) {
	path := rootPath + common.Sep + topic
	exists, err := afero.Exists(afs, path)
	if err != nil {
		return false, errore.Wrap(err)
	}
	if exists {
		return false, nil
	}
	err = afs.Mkdir(path, 0744)
	if err != nil {
		// another caller may have created it after the check
		if exists, _ := afero.DirExists(afs, path); exists {
			return false, nil
		}
		return false, errore.Wrap(err)
	}
	return true, nil
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
	byteOffset := startAtByteOffset
	for {
		entry, n, err := common.ReadEntry(reader, common.MaxEntrySize)
		if err == io.EOF {
			return 0, scanCount, NoByteOffsetFound
		}
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		byteOffset = byteOffset + int64(n)
		scanCount = scanCount + 1
		if common.Offset(entry.Offset+1) == offset {
			return byteOffset, scanCount, nil
		}
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

// ReadEntryAt reads and verifies the entry starting at byteOffset in a log block file.
func ReadEntryAt(afs *afero.Afero, fileName string, byteOffset int64) (common.LogEntry, int, error) {
	file, err := common.OpenFileForRead(afs, fileName)
	if err != nil {
		return common.LogEntry{}, 0, errore.Wrap(err)
	}
	defer file.Close()
	if _, err = file.Seek(byteOffset, io.SeekStart); err != nil {
		return common.LogEntry{}, 0, errore.Wrap(err)
	}
	entry, n, err := common.ReadEntry(file, common.MaxEntrySize)
	if err != nil {
		return common.LogEntry{}, 0, errore.Wrap(err)
	}
	return entry, n, nil
}

// RecoverBlock verifies every entry of a log block from the start and truncates a torn
// tail left by an interrupted write. It returns the offset following the last valid entry,
// the valid size of the block in bytes, and the number of bytes truncated. A valid entry
// with an unexpected offset is corruption rather than a torn write and is returned as an
// error without truncating.
func RecoverBlock(afs *afero.Afero, blockFileName string, firstOffset common.Offset) (common.Offset, int64, int64, error) {
	nextOffset, validSize, fileSize, err := scanValidEntries(afs, blockFileName, firstOffset)
	if err != nil {
		return 0, 0, 0, err
	}
	truncated := fileSize - validSize
	if truncated == 0 {
		return nextOffset, validSize, 0, nil
	}
	file, err := afs.OpenFile(blockFileName, os.O_WRONLY, 0600)
	if err != nil {
		return 0, 0, 0, errore.Wrap(err)
	}
	if err = file.Truncate(validSize); err != nil {
		_ = file.Close()
		return 0, 0, 0, errore.Wrap(err)
	}
	if err = file.Close(); err != nil {
		return 0, 0, 0, errore.Wrap(err)
	}
	return nextOffset, validSize, truncated, nil
}

func scanValidEntries(afs *afero.Afero, blockFileName string, firstOffset common.Offset) (common.Offset, int64, int64, error) {
	file, err := common.OpenFileForRead(afs, blockFileName)
	if err != nil {
		return 0, 0, 0, errore.Wrap(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, 0, 0, errore.Wrap(err)
	}
	reader := bufio.NewReader(file)
	nextOffset := firstOffset
	var validSize int64
	for {
		// a payload cannot be larger than what is left of the file
		var maxSize uint64
		if remaining := info.Size() - validSize; remaining >= common.EntryOverhead {
			maxSize = uint64(remaining - common.EntryOverhead)
		}
		entry, n, err := common.ReadEntry(reader, maxSize)
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, common.ErrCorruptEntry) {
			return nextOffset, validSize, info.Size(), nil
		}
		if err != nil {
			return 0, 0, 0, errore.Wrap(err)
		}
		if common.Offset(entry.Offset) != nextOffset {
			return 0, 0, 0, errore.NewF("log block %s: entry at byte %d has offset %d, expected %d",
				blockFileName, validSize, entry.Offset, nextOffset)
		}
		nextOffset = nextOffset + 1
		validSize = validSize + int64(n)
	}
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
	if params.BatchSize == 0 {
		return ReadResult{}, errore.New("batch size must be greater than zero")
	}
	var currentOffset common.Offset = 0
	var offsetFromLogg common.Offset = 0
	var entriesRead uint64 = 0
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
	batchCapacity := params.BatchSize
	if batchCapacity > maxBatchPrealloc {
		batchCapacity = maxBatchPrealloc
	}
	logEntries := make([]common.LogEntry, 0, batchCapacity)
	currentBatchInBytes := 0
	sendBatch := func() {
		params.Wg.Add(1)
		batch := logEntries
		params.LogChan <- &batch
		logEntries = make([]common.LogEntry, 0, batchCapacity)
		currentBatchInBytes = 0
	}
	for {
		if currentOffset == params.EndOffset {
			if len(logEntries) > 0 {
				sendBatch()
			}
			return ReadResult{
				LastLogOffset: offsetFromLogg,
				EntriesRead:   entriesRead,
			}, nil
		}
		if len(logEntries) == int(params.BatchSize) || currentBatchInBytes > maxBatchBytes {
			sendBatch()
		}
		entry, _, err := common.ReadEntry(reader, common.MaxEntrySize)
		if err == io.EOF {
			if len(logEntries) > 0 {
				sendBatch()
			}
			return ReadResult{
				LastLogOffset: offsetFromLogg,
				EntriesRead:   entriesRead,
			}, nil
		}
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		offsetFromLogg = common.Offset(entry.Offset)
		if currentOffset == 0 {
			currentOffset = offsetFromLogg
		}
		if currentOffset != offsetFromLogg {
			return ReadResult{}, errore.NewF("read order assertion failed, expected [%d] actual [%d]", currentOffset, offsetFromLogg)
		}
		logEntries = append(logEntries, entry)
		currentBatchInBytes = currentBatchInBytes + entry.ByteSize
		entriesRead = entriesRead + 1
		currentOffset = currentOffset + 1
	}
}

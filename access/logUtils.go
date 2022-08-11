package access

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/utils"
	"hash/crc32"
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

const Sep = string(os.PathSeparator)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func openFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return f, nil
}

func OpenFileForRead(afs *afero.Afero, fileName string) (afero.File, error) {
	exists, err := afs.Exists(fileName)
	if err != nil {
		return nil, errore.NewF(fmt.Sprintf("Failes checking if file %s exist", fileName))
	}
	if !exists {
		return nil, errore.NewF(fmt.Sprintf("File %s does not exist", fileName))
	}
	f, err := afs.OpenFile(fileName, os.O_RDONLY, 0400)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return f, nil
}

func CreateTopic(afs *afero.Afero, rootPath string, topic string) error {
	return afs.Mkdir(rootPath+Sep+topic, 0744)
}

func ListAllFilesInTopic(afs *afero.Afero, rootPath string, topic string) ([]os.FileInfo, error) {
	dir, err := OpenFileForRead(afs, rootPath+Sep+topic)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	defer dir.Close()
	return dir.Readdir(0)
}

func LoadTopicBlocks(afs *afero.Afero, rootPath string, topic string) ([]LogBlock, []IndexBlock, error) {
	filesInTopic, err := ListAllFilesInTopic(afs, rootPath, topic)
	if err != nil {
		return nil, nil, errore.Wrap(err)
	}
	var indexBlocks []IndexBlock
	var logBlocks []LogBlock
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
			logBlocks = append(logBlocks, LogBlock(parseUint))
		}
		if fileExtension == ".idx" {
			indexBlocks = append(indexBlocks, IndexBlock(parseUint))
		}
	}
	sort.Slice(indexBlocks, func(i, j int) bool { return indexBlocks[i] < indexBlocks[j] })
	sort.Slice(logBlocks, func(i, j int) bool { return logBlocks[i] < logBlocks[j] })
	return logBlocks, indexBlocks, nil
}

func ListAllTopics(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
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

func FindByteOffsetFromAndIncludingOffset(afs *afero.Afero, fileName string, startAtByteOffset int64, offset Offset) (int64, int, error) {
	scanCount := 0
	if offset == 0 {
		return 0, 0, nil
	}
	file, err := OpenFileForRead(afs, fileName)
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
			return startAtByteOffset, 0, errore.Wrap(err)
		}
	}

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)

	var offsetInFile int64 = math.MaxInt64 - 1
	var byteOffset = startAtByteOffset
	for {
		if Offset(offsetInFile+1) == offset {
			return byteOffset, scanCount, nil
		}
		checksumBytes, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			return 0, scanCount, errore.Wrap(err)
		}
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		sizeBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		entrySize := littleEndianToUint64(bytes)
		entryBytes := make([]byte, entrySize)
		_, err = io.ReadFull(reader, entryBytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		offsetBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		offsetInFile = int64(littleEndianToUint64(bytes))
		byteOffset = byteOffset + int64(offsetBytes) + int64(checksumBytes) + int64(sizeBytes) + int64(entrySize)
		scanCount = scanCount + 1
	}
}

func offsetLookBack(file afero.File) (Offset, error) {
	_, err := file.Seek(-8, io.SeekCurrent)
	if err != nil {
		return 0, errore.Wrap(err)
	}
	bytes := make([]byte, 8)
	_, err = io.ReadFull(file, bytes)
	if err != nil {
		return 0, errore.Wrap(err)
	}
	return Offset(littleEndianToUint64(bytes)), nil
}

func BlockInfo(afs *afero.Afero, blockFileName string) (Offset, int64, error) {
	file, err := OpenFileForRead(afs, blockFileName)
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
	offset := int64(littleEndianToUint64(bytes))
	return Offset(offset), fileSize, nil
}

func ReadFile(file afero.File, logChan chan *[]LogEntry, wg *sync.WaitGroup, batchSize uint32, byteOffset int64, endOffset Offset) (uint64, error) {
	var expectedOffset Offset = 0
	var offsetFromLogg Offset = 0
	var entriesRead uint64 = 0
	if byteOffset > 0 {
		_, err := file.Seek(byteOffset, io.SeekStart)
		if err != nil {
			return entriesRead, errore.Wrap(err)
		}
		offsetFromLogg, err = offsetLookBack(file)
		if err != nil {
			return entriesRead, errore.Wrap(err)
		}
		expectedOffset = offsetFromLogg + 1
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	logEntries := make([]LogEntry, batchSize)
	slicePointer := 0
	for {
		if expectedOffset == endOffset {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			return entriesRead, nil
		}
		if slicePointer != 0 && uint32(slicePointer)%batchSize == 0 {
			wg.Add(1)
			sendingEntries := logEntries[:slicePointer]
			logEntryCopy := make([]LogEntry, slicePointer)
			copy(logEntryCopy, sendingEntries)
			logChan <- &logEntryCopy
			slicePointer = 0
		}
		// Checksum
		_, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			return 0, nil
		}
		if err != nil {
			return entriesRead, nil
		}
		checksumValue := littleEndianToUint32(bytes)

		// Entry size
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return entriesRead, nil
		}

		size := littleEndianToUint64(bytes)

		// Entry bytes
		entry := make([]byte, size)

		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return entriesRead, nil
		}

		// offset
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return entriesRead, nil
		}

		offset := int64(littleEndianToUint64(bytes))
		offsetFromLogg = Offset(offset)
		if expectedOffset == 0 {
			expectedOffset = offsetFromLogg
		}
		if expectedOffset != offsetFromLogg {
			return entriesRead, errore.NewF("read order assertion failed, expected [%d] actual [%d]", expectedOffset, offsetFromLogg)
		}
		logEntries[slicePointer] = LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		}
		entriesRead = entriesRead + 1
		expectedOffset = expectedOffset + 1
		slicePointer = slicePointer + 1
	}
}

func CreateByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(byteSize, crc32q)
	checksum = crc32.Update(checksum, crc32q, entry)
	checksum = crc32.Update(checksum, crc32q, offset)
	check := uint32ToLittleEndian(checksum)
	return utils.JoinSize(20+entrySize, check, byteSize, entry, offset)
}

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

func uint64ToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

func littleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func littleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

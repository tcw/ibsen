package access

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
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

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func openFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func OpenFileForRead(afs *afero.Afero, fileName string) (afero.File, error) {
	exists, err := afs.Exists(fileName)
	if err != nil {
		return nil, errore.NewWithContext(fmt.Sprintf("Failes checking if file %s exist", fileName))
	}
	if !exists {
		return nil, errore.NewWithContext(fmt.Sprintf("File %s does not exist", fileName))
	}
	f, err := afs.OpenFile(fileName, os.O_RDONLY, 0400)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func listFilesInDirectory(afs *afero.Afero, dir string, fileExtension string) ([]string, error) {
	filenames := make([]string, 0)
	file, err := OpenFileForRead(afs, dir)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	names, err := file.Readdirnames(0)
	for _, name := range names {
		hasSuffix := strings.HasSuffix(name, fileExtension)
		if hasSuffix {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func ListAllFilesInTopic(afs *afero.Afero, rootPath string, topic string) ([]os.FileInfo, error) {
	dir, err := OpenFileForRead(afs, rootPath+Sep+topic)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer dir.Close()
	return dir.Readdir(0)
}

func LoadTopicBlocks(afs *afero.Afero, rootPath string, topic string) ([]LogBlock, []IndexBlock, error) {
	filesInTopic, err := ListAllFilesInTopic(afs, rootPath, topic)
	if err != nil {
		return nil, nil, errore.WrapWithContext(err)
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
			return nil, nil, errore.WrapWithContext(err)
		}
		if fileExtension == "log" {
			logBlocks = append(logBlocks, LogBlock(parseUint))
		}
		if fileExtension == "idx" {
			indexBlocks = append(indexBlocks, IndexBlock(parseUint))
		}
	}
	sort.Slice(indexBlocks, func(i, j int) bool { return indexBlocks[i] < indexBlocks[j] })
	sort.Slice(logBlocks, func(i, j int) bool { return logBlocks[i] < logBlocks[j] })
	return logBlocks, indexBlocks, nil
}

func filesToBlocks(paths []string) ([]LogBlock, error) {
	blocks := make([]LogBlock, 0)
	for _, path := range paths {
		_, file := filepath.Split(path)
		ext := filepath.Ext(file)
		fileNameStem := strings.TrimSuffix(file, ext)
		mod, err := strconv.ParseUint(fileNameStem, 10, 64)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		blocks = append(blocks, LogBlock(mod))
	}
	return blocks, nil
}

func listAllTopics(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
	if err != nil {
		return nil, errore.WrapWithContext(err)
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

func FindByteOffsetFromOffset(afs *afero.Afero, fileName string, startAtByteOffset int64, offset Offset) (int64, error) {
	file, err := OpenFileForRead(afs, fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if startAtByteOffset > 0 {
		_, err = file.Seek(startAtByteOffset, io.SeekStart)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)

	var offsetInFile int64 = math.MaxInt64 - 1
	var byteOffset = startAtByteOffset
	for {
		if Offset(offsetInFile+1) == offset {
			return byteOffset, nil
		}
		checksumBytes, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			return 0, err
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		sizeBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		entrySize := littleEndianToUint64(bytes)
		entryBytes := make([]byte, entrySize)
		_, err = io.ReadFull(reader, entryBytes)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		offsetBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		offsetInFile = int64(littleEndianToUint64(bytes))
		byteOffset = byteOffset + int64(offsetBytes) + int64(checksumBytes) + int64(sizeBytes) + int64(entrySize)
	}
}

func FileSize(asf *afero.Afero, fileName string) (int64, error) {
	exists, err := asf.Exists(fileName)
	if err != nil {
		return 0, errore.NewWithContext(fmt.Sprintf("Failes checking if file %s exist", fileName))
	}
	if !exists {
		return 0, errore.NewWithContext(fmt.Sprintf("File %s does not exist", fileName))
	}

	file, err := asf.OpenFile(fileName,
		os.O_RDONLY, 0400)
	fi, err := file.Stat()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	err = file.Close()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return fi.Size(), nil
}

func FindBlockInfo(afs *afero.Afero, blockFileName string) (Offset, int64, error) {
	file, err := OpenFileForRead(afs, blockFileName)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	seek, err := file.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	fileSize := seek + 8
	bytes := make([]byte, 8)
	_, err = io.ReadFull(file, bytes)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	offset := int64(littleEndianToUint64(bytes))
	return Offset(offset), fileSize, nil
}

func ReadFile(file afero.File, logChan chan *[]LogEntry, wg *sync.WaitGroup, batchSize uint32, byteOffset int64, endOffset Offset) error {
	if byteOffset > 0 {
		_, err := file.Seek(byteOffset, io.SeekStart)
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	logEntries := make([]LogEntry, batchSize)
	var currentOffset Offset = 0
	slicePointer := 0
	for {
		if slicePointer != 0 && uint32(slicePointer)%batchSize == 0 {
			wg.Add(1)
			sendingEntries := logEntries[:slicePointer]
			logChan <- &sendingEntries
			logEntries = make([]LogEntry, batchSize)
			slicePointer = 0
		}
		if currentOffset >= endOffset {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			return nil
		}
		// Checksum
		_, err := io.ReadFull(reader, checksum)
		if err == io.EOF {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			return nil
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		checksumValue := littleEndianToUint32(bytes)

		// Entry size
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		size := littleEndianToUint64(bytes)

		// Entry bytes
		entry := make([]byte, size)

		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		// offset
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		offset := int64(littleEndianToUint64(bytes))
		currentOffset = Offset(offset)

		logEntries[slicePointer] = LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		}
		slicePointer = slicePointer + 1
	}
}

func createByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(byteSize, crc32q)
	checksum = crc32.Update(checksum, crc32q, entry)
	checksum = crc32.Update(checksum, crc32q, offset)
	check := uint32ToLittleEndian(checksum)
	return JoinSize(20+entrySize, check, byteSize, entry, offset)
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

func uint16ToLittleEndian(number uint16) []byte {
	bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(bytes, number)
	return bytes
}

func littleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func littleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

func littleEndianToUint16(bytes []byte) uint16 {
	return binary.LittleEndian.Uint16(bytes)
}

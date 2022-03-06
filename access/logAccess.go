package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"io"
	"math"
	"sync"
)

const ByteSumHeaderSize = 20

type LogAccess interface {
	ListTopics() ([]Topic, error)
	Write(fileName FileName, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error)
	Read(fileName FileName, readBatchParam ReadParams, byteOffset int64, nextOffset Offset) (Offset, error)
	ReadTopicLogBlocks(topic Topic) (Blocks, error)
}

var _ LogAccess = ReadWriteLogAccess{}

type ReadWriteLogAccess struct {
	Afs      *afero.Afero
	RootPath string
}

func (la ReadWriteLogAccess) ListTopics() ([]Topic, error) {
	topics, err := listAllTopics(la.Afs, la.RootPath)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return topics, err
}

func (la ReadWriteLogAccess) Write(fileName FileName, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error) {

	writer, err := openFileForWrite(la.Afs, string(fileName))
	defer writer.Close()
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	newOffset, bytesWritten, err := writeBatchToFile(writer, entries, fromOffset)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	return newOffset, bytesWritten, nil
}

func (la ReadWriteLogAccess) ReadTopicLogBlocks(topic Topic) (Blocks, error) {
	directory, err := listFilesInDirectory(la.Afs, la.RootPath+Sep+string(topic), ".log")
	if err != nil {
		return Blocks{}, errore.WrapWithContext(err)
	}
	blocks, err := filesToBlocks(directory)
	domainBlocks := Blocks{BlockList: blocks}
	domainBlocks.Sort()
	return domainBlocks, nil
}

func (la ReadWriteLogAccess) Read(fileName FileName, readBatchParam ReadParams, byteOffset int64, currentOffset Offset) (Offset, error) {
	logFile, err := OpenFileForRead(la.Afs, string(fileName))
	defer logFile.Close()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	if readBatchParam.Offset > 0 {
		if byteOffset > 0 {
			_, err = logFile.Seek(byteOffset, io.SeekStart)
			if err != nil {
				return 0, errore.WrapWithContext(err)
			}
		}
		offset, err := ReadOffset(la.Afs, fileName, byteOffset)
		if err == io.EOF {
			return 0, io.EOF
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		if offset < readBatchParam.Offset {
			err = fastForwardToOffset(logFile, readBatchParam.Offset, currentOffset)
			if err == io.EOF {
				return readBatchParam.Offset, nil
			}
			if err != nil {
				return 0, errore.WrapWithContext(err)
			}
		}
	}

	lastOffset, err := ReadFile(logFile, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize, currentOffset)
	if err != nil {
		return lastOffset, errore.WrapWithContext(err)
	}
	return lastOffset, nil
}

func ReadFile(file afero.File, logChan chan *[]LogEntry, wg *sync.WaitGroup, batchSize uint32, currentOffset Offset) (Offset, error) {
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	logEntries := make([]LogEntry, batchSize)
	slicePointer := 0
	var lastOffset Offset = math.MaxUint64
	readAtLeastOneEntry := false

	for {
		if slicePointer != 0 && uint32(slicePointer)%batchSize == 0 {
			wg.Add(1)
			sendingEntries := logEntries[:slicePointer]
			logChan <- &sendingEntries
			logEntries = make([]LogEntry, batchSize)
			slicePointer = 0
		}
		if lastOffset != math.MaxUint64 && lastOffset == currentOffset {
			if slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			if readAtLeastOneEntry {
				return lastOffset + 1, nil
			}
			return lastOffset, nil
		}
		_, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			if readAtLeastOneEntry {
				return lastOffset + 1, nil
			}
			return lastOffset, nil
		}
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		offset := int64(littleEndianToUint64(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		checksumValue := littleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)

		entry := make([]byte, size)

		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}

		logEntries[slicePointer] = LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		}
		slicePointer = slicePointer + 1
		lastOffset = Offset(offset)
		readAtLeastOneEntry = true
	}
}

func writeBatchToFile(file afero.File, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error) {
	currentOffset := fromOffset
	neededAllocation := 0
	for _, entry := range *entries {
		neededAllocation = neededAllocation + len(entry) + ByteSumHeaderSize
	}
	var bytes = make([]byte, neededAllocation)
	start := 0
	end := 0
	for _, entry := range *entries {
		byteEntry := createByteEntry(entry, currentOffset)
		end = start + len(byteEntry)
		copy(bytes[start:end], byteEntry)
		start = start + len(byteEntry)
		currentOffset = currentOffset + 1
	}
	n, err := file.Write(bytes)
	if err != nil {
		return currentOffset, 0, errore.WrapWithContext(err)
	}
	return currentOffset, BlockSizeInBytes(n), nil
}

func createByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := uint32ToLittleEndian(checksum)
	return JoinSize(ByteSumHeaderSize+entrySize, offset, check, byteSize, entry)
}

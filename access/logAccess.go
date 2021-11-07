package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"io"
	"sync"
)

type LogAccess interface {
	ListTopics() ([]Topic, error)
	CreateTopic(topic Topic) error
	Write(fileName FileName, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error)
	ReadTopicLogBlocks(topic Topic) (Blocks, error)
	ReadLog(logFile afero.File, readBatchParam ReadParams) (Offset, error)
}

var _ LogAccess = ReadWriteLogAccess{}

type ReadWriteLogAccess struct {
	Afs      *afero.Afero
	RootPath string
}

func (la ReadWriteLogAccess) ListTopics() ([]Topic, error) {
	topics, err := ListAllTopics(la.Afs, la.RootPath)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return topics, err
}

func (la ReadWriteLogAccess) CreateTopic(topic Topic) error {
	err := la.Afs.Mkdir(string(topic), 640)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (la ReadWriteLogAccess) Write(fileName FileName, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error) {

	writer, err := OpenFileForWrite(la.Afs, string(fileName))
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	newOffset, bytesWritten, err := writeBatchToFile(writer, entries, fromOffset)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	err = writer.Close()
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	return newOffset, bytesWritten, nil
}

func (la ReadWriteLogAccess) ReadTopicLogBlocks(topic Topic) (Blocks, error) {
	directory, err := ListFilesInDirectory(la.Afs, la.RootPath, string(topic))
	if err != nil {
		return Blocks{}, errore.WrapWithContext(err)
	}
	blocks, err := FilesToBlocks(directory)
	domainBlocks := Blocks{BlockList: blocks}
	domainBlocks.Sort()
	return domainBlocks, nil
}

func (la ReadWriteLogAccess) ReadLog(logFile afero.File, readBatchParam ReadParams) (Offset, error) {
	if readBatchParam.Offset > 0 {
		if readBatchParam.ByteOffset > 0 {
			_, err := logFile.Seek(readBatchParam.ByteOffset, io.SeekStart)
			if err != nil {
				return 0, errore.WrapWithContext(err)
			}
		}
		err := FastForwardToOffset(logFile, readBatchParam.Offset)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}

	lastOffset, err := readFile(logFile, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		return lastOffset, errore.WrapWithContext(err)
	}
	return lastOffset, nil
}

func readFile(file afero.File, logChan chan *[]LogEntry,
	wg *sync.WaitGroup, batchSize uint32) (Offset, error) {

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	var logEntries []LogEntry
	var lastOffset Offset

	for {
		if logEntries != nil && uint32(len(logEntries))%batchSize == 0 {
			wg.Add(1)
			logChan <- &logEntries
			logEntries = nil
		}
		_, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			if logEntries != nil {
				wg.Add(1)
				logChan <- &logEntries
			}
			return lastOffset, nil
		}
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		offset := int64(LittleEndianToUint64(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		checksumValue := LittleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		size := LittleEndianToUint64(bytes)

		entry := make([]byte, size)
		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		logEntries = append(logEntries, LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		})
		lastOffset = Offset(offset)
	}
}

func writeBatchToFile(file afero.File, entries Entries, fromOffset Offset) (Offset, BlockSizeInBytes, error) {
	var bytes []byte
	currentOffset := fromOffset
	for _, entry := range *entries {
		bytes = append(bytes, createByteEntry(entry, currentOffset)...)
		currentOffset = currentOffset + 1
	}
	n, err := file.Write(bytes)
	if err != nil {
		return currentOffset, 0, errore.WrapWithContext(err)
	}
	return currentOffset, BlockSizeInBytes(n), nil
}

func createByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := Uint64ToLittleEndian(uint64(currentOffset))
	byteSize := IntToLittleEndian(len(entry))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := Uint32ToLittleEndian(checksum)
	bytes := append(offset, check...)
	bytes = append(bytes, byteSize...)
	bytes = append(bytes, entry...)
	return bytes
}

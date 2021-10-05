package access

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"io"
	"sync"
)

type ReadParams struct {
	LogChan   chan *LogEntryBatch
	Wg        *sync.WaitGroup
	Topic     Topic
	BatchSize NumberOfEntries
	Offset    Offset
}

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type LogEntryBatch struct {
	Entries []LogEntry
}

func ReadFileFromLogOffset(file afero.File, readBatchParam ReadParams) (Offset, error) {
	if readBatchParam.Offset > 0 {
		err := fastForwardToOffset(file, readBatchParam.Offset)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}

	lastOffset, err := ReadFile(file, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		return lastOffset, errore.WrapWithContext(err)
	}
	return lastOffset, nil
}

func ReadFile(file afero.File, logChan chan *LogEntryBatch,
	wg *sync.WaitGroup, batchSize NumberOfEntries) (Offset, error) {

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	var entryBatch []LogEntry
	var lastOffset Offset

	for {
		if entryBatch != nil && uint32(len(entryBatch))%uint32(batchSize) == 0 {
			wg.Add(1)
			logChan <- &LogEntryBatch{Entries: entryBatch}
			entryBatch = nil
		}
		_, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			if entryBatch != nil {
				wg.Add(1)
				logChan <- &LogEntryBatch{Entries: entryBatch}
			}
			return lastOffset, nil
		}
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		offset := int64(commons.LittleEndianToUint64(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		checksumValue := commons.LittleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		size := commons.LittleEndianToUint64(bytes)

		entry := make([]byte, size)
		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return lastOffset, errore.WrapWithContext(err)
		}
		entryBatch = append(entryBatch, LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		})
		lastOffset = Offset(offset)
	}
}

func fastForwardToOffset(file afero.File, offset Offset) error { //Todo: replace with index
	var offsetFound Offset = -1
	for {
		if offsetFound == offset {
			return nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return errore.NewWithContext("no Offset in block")
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		offsetFound = Offset(commons.LittleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		size := commons.LittleEndianToUint64(bytes)
		_, err = (file).Seek(int64(size), 1)
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
}

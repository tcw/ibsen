package ext4

import (
	"bufio"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/logStorage"
	"io"
	"os"
	"sync"
)

func ReadLogBlockFromOffsetNotIncluding(file *os.File, readBatchParam logStorage.ReadBatchParam) error {

	if readBatchParam.Offset > 0 {
		err := fastForwardToOffset(file, int64(readBatchParam.Offset))
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}

	partialBatch, _, err := ReadLogInBatchesToEnd(file, nil, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if partialBatch.Entries != nil {
		readBatchParam.Wg.Add(1)
		readBatchParam.LogChan <- &logStorage.LogEntryBatch{Entries: partialBatch.Entries}
	}
	return nil
}

func ReadLogInBatchesToEnd(file *os.File, partialBatch []logStorage.LogEntry, logChan chan *logStorage.LogEntryBatch,
	wg *sync.WaitGroup, batchSize int) (logStorage.LogEntryBatch, bool, error) {

	hasSent := false
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	var entryBatch []logStorage.LogEntry
	if partialBatch != nil {
		entryBatch = append(entryBatch, partialBatch...)
	}
	for {
		if entryBatch != nil && len(entryBatch)%batchSize == 0 {
			wg.Add(1)
			logChan <- &logStorage.LogEntryBatch{Entries: entryBatch}
			hasSent = true
			entryBatch = nil
		}

		_, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return logStorage.LogEntryBatch{Entries: entryBatch}, hasSent, nil
		}
		if err != nil {
			return logStorage.LogEntryBatch{}, false, errore.WrapWithContext(err)
		}
		offset := int64(fromLittleEndian(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return logStorage.LogEntryBatch{}, false, errore.WrapWithContext(err)
		}
		checksumValue := fromLittleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return logStorage.LogEntryBatch{}, false, errore.WrapWithContext(err)
		}
		size := fromLittleEndian(bytes)

		entry := make([]byte, size)
		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return logStorage.LogEntryBatch{}, false, errore.WrapWithContext(err)
		}
		entryBatch = append(entryBatch, logStorage.LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		})
	}
}

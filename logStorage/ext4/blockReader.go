package ext4

import (
	"bufio"
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"io"
	"log"
	"os"
	"sync"
)

func ReadLogBlockFromOffsetNotIncluding(file *os.File, logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int, offset uint64) error {

	if offset > 0 {
		err := fastForwardToOffset(file, int64(offset))
		if err != nil {
			return err
		}
	}

	partialBatch, _, err := ReadLogInBatchesToEnd(file, nil, logChan, wg, batchSize)
	if err != nil {
		return err
	}
	if partialBatch.Entries != nil {
		wg.Add(1)
		logChan <- logStorage.LogEntryBatch{Entries: partialBatch.Entries}
	}
	return nil
}

func ReadLogInBatchesToEnd(file *os.File, partialBatch []logStorage.LogEntry, logChan chan logStorage.LogEntryBatch,
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
			logChan <- logStorage.LogEntryBatch{Entries: entryBatch}
			hasSent = true
			entryBatch = nil
		}

		n, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return logStorage.LogEntryBatch{Entries: entryBatch}, hasSent, nil
		}
		if err != nil {
			log.Println(err)
			return logStorage.LogEntryBatch{}, false, err
		}
		if n != 8 {
			log.Println("offset incorrect")
			return logStorage.LogEntryBatch{}, false, errors.New("offset incorrect")
		}
		offset := int64(fromLittleEndian(bytes))

		n, err = io.ReadFull(reader, checksum)
		if err != nil {
			log.Println(err)
			return logStorage.LogEntryBatch{}, false, err
		}
		if n != 4 {
			log.Println("crc size incorrect")
			return logStorage.LogEntryBatch{}, false, errors.New("crc size incorrect")
		}
		checksumValue := fromLittleEndianToUint32(bytes)

		n, err = io.ReadFull(reader, bytes)
		if err != nil {
			log.Println(err)
			return logStorage.LogEntryBatch{}, false, err
		}
		if n != 8 {
			log.Println("entry size incorrect")
			return logStorage.LogEntryBatch{}, false, errors.New("entry size incorrect")
		}
		size := fromLittleEndian(bytes)

		entry := make([]byte, size)
		n, err = io.ReadFull(reader, entry)
		if err != nil {
			log.Println(err)
			return logStorage.LogEntryBatch{}, false, err
		}
		if n != int(size) {
			log.Println("entry incorrect")
			return logStorage.LogEntryBatch{}, false, errors.New("entry incorrect")
		}
		entryBatch = append(entryBatch, logStorage.LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		})
	}
}

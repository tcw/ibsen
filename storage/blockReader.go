package storage

import (
	"bufio"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"io"
	"os"
	"sync"
)

func ReadFileFromLogInternalOffset(file afero.File, readBatchParam ReadBatchParam, internalOffset int64) error {
	_, err := file.Seek(internalOffset, io.SeekStart)
	if err != nil {
		return errore.WrapWithContext(err)
	}

	err = ReadFile(file, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func ReadFileFromLogOffset(file afero.File, readBatchParam ReadBatchParam) error {

	if readBatchParam.Offset > 0 {
		err := fastForwardToOffset(file, int64(readBatchParam.Offset))
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}

	err := ReadFile(file, readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func ReadFile(file afero.File, logChan chan *LogEntryBatch,
	wg *sync.WaitGroup, batchSize int) error {

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	var entryBatch []LogEntry

	for {
		if entryBatch != nil && len(entryBatch)%batchSize == 0 {
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
			return nil
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		offset := int64(commons.LittleEndianToUint64(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		checksumValue := commons.LittleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		size := commons.LittleEndianToUint64(bytes)

		entry := make([]byte, size)
		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		entryBatch = append(entryBatch, LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		})
	}
}

func blockSize(asf *afero.Afero, fileName string) (int64, error) {
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

func findLastOffset(afs *afero.Afero, blockFileName string) (int64, error) {
	var offsetFound int64 = 0
	file, err := commons.OpenFileForRead(afs, blockFileName)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	defer file.Close()
	for {
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return offsetFound, nil
		}
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		offsetFound = int64(commons.LittleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		size := commons.LittleEndianToUint64(bytes)
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
	}
}

func fastForwardToOffset(file afero.File, offset int64) error {
	var offsetFound int64 = -1
	for {
		if offsetFound == offset {
			return nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return errore.NewWithContext("no offset in block")
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		offsetFound = int64(commons.LittleEndianToUint64(bytes))
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

type OffsetPosition struct {
	Offset     uint64
	ByteOffset uint64
}

func ReadOffsetAndByteOffset(file afero.File, maxEntriesFound int, modulo uint32) ([]OffsetPosition, error) {
	var entriesFound int = 0
	var offsets []OffsetPosition
	var offset uint64
	var byteSum int64 = 0
	for {
		if entriesFound >= maxEntriesFound {
			return offsets, nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		byteSum = byteSum + 8
		if err == io.EOF {
			return offsets, nil
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		offset = commons.LittleEndianToUint64(bytes)
		_, err = io.ReadFull(file, checksum)
		byteSum = byteSum + 4
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		byteSum = byteSum + 8
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		size := commons.LittleEndianToUint64(bytes)
		byteSum = byteSum + int64(size)
		_, err = (file).Seek(int64(size), 1)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		if (offset)%uint64(modulo) == 0 { //todo: this is wrong
			offsets = append(offsets, OffsetPosition{
				Offset:     offset,
				ByteOffset: uint64(byteSum),
			})
			entriesFound = entriesFound + 1
		}
	}
}

package storage

import (
	"bufio"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"io"
	"os"
	"strconv"
	"strings"
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
		offset := int64(littleEndianToUint64(bytes))

		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		checksumValue := littleEndianToUint32(bytes)

		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)

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

func createBlockFileName(blockName int64) string {
	return fmt.Sprintf("%020d.log", blockName)
}

func findLastOffset(afs *afero.Afero, blockFileName string) (int64, error) {
	var offsetFound int64 = -1
	file, err := OpenFileForRead(afs, blockFileName)
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

		if err == io.EOF {
			return offsetFound, errore.NewWithContext("no offset in block")
		}
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		offsetFound = int64(littleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)
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
		offsetFound = int64(littleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)
		_, err = (file).Seek(int64(size), 1)
		if err != nil {
			println(err)
			return err
		}
	}
}

func filesToBlocks(files []string) ([]int64, error) {
	var blocks []int64
	for _, file := range files {
		splitFileName := strings.Split(file, ".")
		if len(splitFileName) != 2 {
			continue
		}
		if splitFileName[1] == "log" {
			splitPath := strings.Split(splitFileName[0], separator)
			parseUint, err := strconv.ParseInt(splitPath[len(splitPath)-1], 10, 64)
			if err != nil {
				return nil, errore.WrapWithContext(err)
			}
			blocks = append(blocks, parseUint)
		}
	}
	return blocks, nil
}

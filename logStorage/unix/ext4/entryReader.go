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

func NewLogReader(FileName string) (*LogFile, error) {
	lw := new(LogFile)
	lw.FileName = FileName

	f, err := os.OpenFile(lw.FileName,
		os.O_RDONLY, 0400)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	lw.LogFile = f
	return lw, nil
}

func (lw *LogFile) ReadCurrentOffset() (uint64, error) {
	var offsetFound uint64 = 0

	for {
		bytes := make([]byte, 8)
		n, err := lw.LogFile.Read(bytes)
		if err == io.EOF {
			return offsetFound, nil
		}
		if err != nil {
			return offsetFound, errors.New("error")
		}
		if n != 8 {
			log.Println("offset incorrect")
		}
		offsetFound = fromLittleEndian(bytes)

		n, err2 := lw.LogFile.Read(bytes)
		if n != 8 {
			log.Println("offset incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err2)
			return 0, err2
		}
		_, err = lw.LogFile.Seek(int64(size), 1)
		if err != nil {
			println(err)
			return 0, err
		}
	}
}

func (lw *LogFile) FastForwardToOffset(offset int64) error {
	var offsetFound int64 = -1
	for {
		if offsetFound == offset {
			return nil
		}
		bytes := make([]byte, 8)
		n, err := lw.LogFile.Read(bytes)
		if err == io.EOF {
			return errors.New("no offset in block")
		}
		if err != nil {
			return errors.New("error")
		}
		if n != 8 {
			log.Println("offset incorrect")
			return errors.New("offset incorrect")
		}
		offsetFound = int64(fromLittleEndian(bytes))

		n, err2 := lw.LogFile.Read(bytes)
		if n != 8 {
			log.Println("offset incorrect")
			return errors.New("offset incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err2)
			return err2
		}
		_, err = lw.LogFile.Seek(int64(size), 1)
		if err != nil {
			println(err)
			return err
		}
	}
}

func (lw *LogFile) ReadLogBlockFromOffsetNotIncluding(logChan chan *logStorage.LogEntryBatch, wg *sync.WaitGroup, offset uint64, batchSize int) (*logStorage.LogEntryBatch, bool, error) {

	err := lw.FastForwardToOffset(int64(offset))
	if err != nil {
		return nil, false, err
	}

	partialBatch, hasSent, err := lw.ReadLogToEnd(nil, logChan, wg, batchSize)
	if err != nil {
		return nil, false, err
	}
	return partialBatch, hasSent, nil
}

func (lw *LogFile) ReadLogToEnd(partialBatch *[]logStorage.LogEntry, logChan chan *logStorage.LogEntryBatch,
	wg *sync.WaitGroup, batchSize int) (*logStorage.LogEntryBatch, bool, error) {

	hasSent := false
	reader := bufio.NewReader(lw.LogFile)
	bytes := make([]byte, 8)
	var entryBatch []logStorage.LogEntry
	entryBatch = append(entryBatch, *partialBatch...)
	for {

		if len(entryBatch)%batchSize == 0 {
			logChan <- &logStorage.LogEntryBatch{Entries: &entryBatch}
			hasSent = true
			wg.Add(1)
			entryBatch = nil
		}

		n, err := io.ReadFull(reader, bytes)

		if err == io.EOF {
			return &logStorage.LogEntryBatch{Entries: &entryBatch}, hasSent, nil
		}
		if err != nil {
			log.Println(err)
			return nil, false, err
		}
		if n != 8 {
			log.Println("offset incorrect")
			return nil, false, errors.New("offset incorrect")
		}
		offset := int64(fromLittleEndian(bytes))

		n, err2 := io.ReadFull(reader, bytes)
		if n != 8 {
			log.Println("entry size incorrect")
			return nil, false, errors.New("entry size incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err2)
			return nil, false, err2
		}
		entry := make([]byte, size)
		n, err3 := io.ReadFull(reader, entry)
		if err3 != nil {
			log.Println(err3)
			return nil, false, err3
		}
		if n != int(size) {
			log.Println("entry incorrect")
			return nil, false, errors.New("entry incorrect")
		}
		entryBatch = append(entryBatch, logStorage.LogEntry{
			Offset:   uint64(offset),
			ByteSize: int(size),
			Entry:    &entry,
		})
	}
}

// Todo: use same approach as over
func (lw *LogFile) ReadLogFromOffsetNotIncluding(c chan *logStorage.LogEntry, excludingOffset uint64) error {
	var offsetFound = false
	skippedFirst := false

	for {
		bytes := make([]byte, 8)
		n, err := lw.LogFile.Read(bytes)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}
		if n != 8 {
			log.Println("offset incorrect")
			return errors.New("offset incorrect")
		}
		offset := fromLittleEndian(bytes)

		n, err2 := lw.LogFile.Read(bytes)
		if n != 8 {
			log.Println("byte size incorrect")
			return errors.New("byte size incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err)
			return err2
		}

		if !offsetFound {
			if excludingOffset != offset {
				_, err := lw.LogFile.Seek(int64(size), 1)
				if err != nil {
					return err
				}
				continue
			} else {
				offsetFound = true
			}
		}

		if offsetFound {
			entry := make([]byte, size)
			n, err3 := lw.LogFile.Read(entry)
			if err3 != nil {
				log.Println(err)
			}
			if n != int(size) {
				log.Println("entry incorrect")
				return errors.New("entry incorrect")
			}
			if skippedFirst {
				c <- &logStorage.LogEntry{
					Offset:   offset,
					ByteSize: int(size),
					Entry:    &entry,
				}
			} else {
				skippedFirst = true
				continue
			}
		}
	}
}

func (lw *LogFile) ReadLogFromBeginning(c chan *logStorage.LogEntry, wg *sync.WaitGroup) error {
	reader := bufio.NewReader(lw.LogFile)
	bytes := make([]byte, 8)
	for {
		n, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}
		if n != 8 {
			log.Println("offset incorrect")
			return errors.New("offset incorrect")
		}
		offset := fromLittleEndian(bytes)
		n, err2 := io.ReadFull(reader, bytes)
		if n != 8 {
			log.Println("entry size incorrect")
			return errors.New("entry size incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err2)
			return err2
		}
		entry := make([]byte, size)
		n, err3 := io.ReadFull(reader, entry)
		if err3 != nil {
			log.Println(err3)
			return err3
		}
		if n != int(size) {
			log.Println("entry incorrect")
			return errors.New("entry incorrect")
		}
		c <- &logStorage.LogEntry{
			Offset:   offset,
			ByteSize: int(size),
			Entry:    &entry,
		}
		wg.Add(1)
	}
}

func (lw *LogFile) CloseLogReader() error {
	return lw.LogFile.Close()
}

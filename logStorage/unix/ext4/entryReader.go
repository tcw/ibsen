package ext4

import (
	"bufio"
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"io"
	"log"
	"os"
)

func NewLogReader(FileName string) *LogFile {
	lw := new(LogFile)
	lw.FileName = FileName

	f, err := os.OpenFile(lw.FileName,
		os.O_RDONLY, 0400)
	if err != nil {
		log.Println(err)
	}
	lw.LogFile = f
	return lw
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
			log.Println(err)
		}
		lw.LogFile.Seek(int64(size), 1)
		if err != nil {
			println(err)
		}
	}
}

func (lw *LogFile) ReadLogFromOffsetNotIncluding(c chan *logStorage.LogEntry, excludingOffset logStorage.Offset) error {
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
			if uint64(excludingOffset) != offset {
				lw.LogFile.Seek(int64(size), 1)
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
					Offset:   logStorage.Offset(offset),
					ByteSize: int(size),
					Entry:    entry,
				}
			} else {
				skippedFirst = true
				continue
			}
		}
	}
}

func (lw *LogFile) ReadLogFromBeginning(c chan *logStorage.LogEntry) error {
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
			Offset:   logStorage.Offset(offset),
			ByteSize: int(size),
			Entry:    entry,
		}
	}
}

func (lw *LogFile) CloseLogReader() {
	lw.LogFile.Close()
}

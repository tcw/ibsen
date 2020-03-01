package ext4

import (
	"bufio"
	"errors"
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

func (lw *LogFile) ReadLogFromOffsetNotIncluding(c chan LogEntry, excludingOffset uint64) uint64 {

	var offsetFound = false
	var readCount uint64 = 0
	skippedFirst := false

	for {
		bytes := make([]byte, 8)
		n, err := lw.LogFile.Read(bytes)
		if err == io.EOF {
			return readCount
		}
		if err != nil {
			log.Println(err)
		}
		if n != 8 {
			log.Println("offset incorrect")
		}
		offset := fromLittleEndian(bytes)

		n, err2 := lw.LogFile.Read(bytes)
		if n != 8 {
			log.Println("offset incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err)
		}

		if !offsetFound {
			if excludingOffset != offset {
				lw.LogFile.Seek(int64(size), 1)
				continue
			} else {
				offsetFound = true
			}
		}

		if offsetFound {
			payload := make([]byte, size)
			n, err3 := lw.LogFile.Read(payload)
			if err3 != nil {
				log.Println(err)
			}
			if n != int(size) {
				log.Println("offset incorrect")
			}
			if skippedFirst {
				c <- LogEntry{Offset: offset, Size: size, Payload: payload}
				readCount = readCount + 1
			} else {
				skippedFirst = true
				continue
			}

		}
	}
}

func (lw *LogFile) ReadLogFromBeginning(c chan LogEntry) uint64 {
	var readCount uint64 = 0
	reader := bufio.NewReader(lw.LogFile)
	bytes := make([]byte, 8)
	for {
		n, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return readCount
		}
		if err != nil {
			log.Println(err)
		}
		if n != 8 {
			log.Println("offset incorrect")
		}
		offset := fromLittleEndian(bytes)
		n, err2 := io.ReadFull(reader, bytes)
		if n != 8 {
			log.Println("payload size incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err)
		}
		payload := make([]byte, size)
		n, err3 := io.ReadFull(reader, payload)
		if err3 != nil {
			log.Println(err)
		}
		if n != int(size) {
			log.Println("payload incorrect")
		}
		c <- LogEntry{Offset: offset, Size: size, Payload: payload}
		readCount = readCount + 1
	}
}

func (lw *LogFile) CloseLogReader() {
	lw.LogFile.Close()
}

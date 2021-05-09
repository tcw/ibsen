package storage

import (
	"fmt"
	"github.com/tcw/ibsen/commons"
	"io"
	"math/rand"
	"testing"
	"time"
)

func TestReadOffsetAndByteOffset(t *testing.T) {
	afs := newAfero()
	writer := BlockWriter{
		Afs:       afs,
		Filename:  "test/00000000000000000000.log",
		LogEntry:  createTestEntries(100, 1),
		Offset:    0,
		BlockSize: 0,
	}
	_, _, err := writer.WriteBatch()
	if err != nil {
		t.Error(err)
	}
	read, err := commons.OpenFileForRead(afs, "test/00000000000000000000.log")
	if err != nil {
		t.Error(err)
	}
	offset, err := ReadOffsetAndByteOffset(read, 0, 1000, 3)
	if err != nil {
		t.Error(err)
	}
	for _, position := range offset {
		file, err := commons.OpenFileForRead(afs, "test/00000000000000000000.log")
		if err != nil {
			t.Error(err)
		}
		_, err = file.Seek(int64(position.ByteOffset), io.SeekStart)
		if err != nil {
			t.Error(err)
		}
		logEntries, err := ReadFileAndReturn(file, 1)
		if err != nil {
			t.Error(err)
		}
		first, err := logEntries.FindFirst()
		if err != nil {
			t.Error(err)
		}
		if position.Offset != first.Offset {
			fmt.Printf("expected : %d actual: %d\n", position.Offset, first.Offset)
			t.FailNow()
		}
	}
}

//todo: this is a duplicate!
func createTestEntries(entries int, entriesByteSize int) [][]byte {
	var bytes = make([][]byte, 0)

	for i := 0; i < entries; i++ {
		entry := createTestValues(entriesByteSize)
		bytes = append(bytes, entry)
	}
	return bytes
}

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())

	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

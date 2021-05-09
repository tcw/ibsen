package storage

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"math/rand"
	"time"
)

func CreateTestLog(afs *afero.Afero, rootPath string, topic string, blockName uint64) (string, error) {
	filename := commons.CreateLogBlockFilename(rootPath, topic, blockName)
	writer := BlockWriter{
		Afs:      afs,
		Filename: filename,
		LogEntry: CreateTestEntries(100, 100),
	}
	offset, _, err := writer.WriteBatch()
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	fmt.Printf("Created log file [%s] with offset head [%d]\n", filename, offset)
	return filename, nil
}

func CreateTestEntries(entries int, entriesByteSize int) [][]byte {
	var bytes = make([][]byte, 0)

	for i := 0; i < entries; i++ {
		entry := CreateTestValues(entriesByteSize)
		bytes = append(bytes, entry)
	}
	return bytes
}

func CreateTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())

	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

package test

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/manager"
	"io"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

const rootPath = "/tmp/data"

var afs *afero.Afero

func setUp() {
	var fs = afero.NewMemMapFs()
	afs = &afero.Afero{Fs: fs}
	err := afs.Mkdir(rootPath, 640)
	if err != nil {
		log.Fatal(err)
	}
}

func readVerification(t *testing.T, logChan chan *[]access.LogEntry, wg *sync.WaitGroup, prefix string, from int) {
	for true {
		entryBatch := <-logChan
		entries := *entryBatch
		counter := from
		for _, entry := range entries {
			expected := prefix + "_" + strconv.Itoa(counter)
			actual := string(entry.Entry)
			counter = counter + 1
			if actual != expected {
				t.Logf("entry content expected %s actual %s", expected, actual)
				t.Fail()
			}
		}
		wg.Done()
	}
}

func readWithoutVerification(logChan chan *[]access.LogEntry, wg *sync.WaitGroup) {
	for true {
		_ = <-logChan
		wg.Done()
	}
}

func writeEvery100ms(handler *manager.TopicHandler, total time.Duration, writeEvery time.Duration) {
	readTTL := time.Now().Add(total)
	for time.Until(readTTL) > 0 {
		_, err := handler.Write(createEntry(10, "hello", 0))
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(writeEvery)
	}
}

func createEntry(entries int, entryPrefix string, from int) access.Entries {
	var bytes [][]byte
	for i := from; i < entries+from; i++ {
		bytes = append(bytes, []byte(fmt.Sprintf("%s_%d", entryPrefix, i)))
	}
	return &bytes
}

func readBytesFromByteOffset(afs *afero.Afero, fileName string, seekFromStart int64, readBytes int) ([]byte, error) {
	reader, err := access.OpenFileForRead(afs, fileName)
	if err != nil {
		return nil, err
	}
	_, err = reader.Seek(seekFromStart, io.SeekStart)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, readBytes)
	_, err = io.ReadFull(reader, bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

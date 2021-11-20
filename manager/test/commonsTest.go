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

func readVerification(t *testing.T, logChan chan *[]access.LogEntry, wg *sync.WaitGroup) {
	counter := 0
	for true {
		entryBatch := <-logChan
		counter = counter + 1
		entries := *entryBatch
		log.Println(strconv.Itoa(counter) + " " + strconv.FormatUint(entries[0].Offset, 10))
		wg.Done()
	}
}

func writeEvery100ms(handler *manager.TopicHandler) {
	for true {
		_, err := handler.Write(createEntry(1000))
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func createEntry(entries int) access.Entries {
	var bytes [][]byte
	for i := 0; i < entries; i++ {
		bytes = append(bytes, []byte(fmt.Sprintf("hello_%d", i)))
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

package test

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"io"
	"log"
	"sync"
	"testing"
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
	entryBatch := <-logChan
	entries := *entryBatch
	if entries[0].Offset != 0 {
		t.Fail()
	}
	wg.Done()
	log.Println("2")
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

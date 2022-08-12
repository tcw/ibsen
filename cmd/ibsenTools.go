package cmd

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"math"
	"sync"
)

func ReadLogFile(fileName string, batchSize uint32) error {
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	var fs = afero.NewOsFs()
	afs := &afero.Afero{Fs: fs}
	go sendBatchMessage(logChan, &wg)
	file, err := access.OpenFileForRead(afs, fileName)
	if err != nil {
		return err
	}
	_, err = access.ReadFile(access.ReadFileParams{
		File:            file,
		LogChan:         logChan,
		Wg:              &wg,
		BatchSize:       batchSize,
		StartByteOffset: 0,
		EndOffset:       math.MaxUint64,
	})
	if err != nil {
		return err
	}
	return nil
}

// Todo: implement
func ReadLogIndexFile(fileName string) error {
	fmt.Println(fileName)
	return nil
}

func sendBatchMessage(logChan chan *[]access.LogEntry, wg *sync.WaitGroup) {
	for {
		entryBatch := <-logChan
		batch := *entryBatch
		for _, entry := range batch {
			fmt.Printf("%d\t%s", entry.Offset, string(entry.Entry))
		}
		wg.Done()
	}
}

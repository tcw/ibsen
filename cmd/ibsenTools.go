package cmd

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
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
		return errore.WrapWithContext(err)
	}
	_, err = access.ReadFile(file, logChan, &wg, batchSize, math.MaxUint64)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func ReadLogIndexFile(fileName string) error {
	var fs = afero.NewOsFs()
	afs := &afero.Afero{Fs: fs}

	binaryIndex, err := access.LoadIndex(afs, fileName)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	index, err := access.MarshallIndex(binaryIndex)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	fmt.Println(index.ToString())
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

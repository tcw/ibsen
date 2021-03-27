package client

import (
	"encoding/base64"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

func ReadTopic(afs *afero.Afero, readPath string, toBase64 bool) {

	if readPath != "" {
		abs, err := filepath.Abs(readPath)
		if err != nil {
			err := errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
		}
		f, err := os.OpenFile(abs,
			os.O_RDONLY, 0400)
		if err != nil {
			err := errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return
		}
		stat, err := f.Stat()
		if err != nil {
			err := errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return
		}
		isDir := stat.IsDir()
		err = f.Close()
		if err != nil {
			err := errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return
		}
		if isDir {
			dir, topic := path.Split(abs)
			logChannel := make(chan *storage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			manger, err := storage.NewBlockManger(afs, dir, topic, 1024*1024*10)
			reader, err := storage.NewTopicReader(afs, &manger)
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			_, _, err = reader.ReadFromOffset(storage.ReadBatchParam{
				LogChan:   logChannel,
				Wg:        &wg,
				Topic:     "",
				BatchSize: 1000,
				Offset:    0,
			})
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			wg.Wait()
		} else {
			logChannel := make(chan *storage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			openFile, err := commons.OpenFileForRead(afs, readPath)
			err = storage.ReadFileFromLogOffset(openFile, storage.ReadBatchParam{
				LogChan:   logChannel,
				Wg:        &wg,
				Topic:     "",
				BatchSize: 1000,
				Offset:    0,
			})
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			err = openFile.Close()
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Println(errore.SprintTrace(err))
			}
			wg.Wait()
		}
	}
}

func writeToStdOut(c chan *storage.LogEntryBatch, wg *sync.WaitGroup, toBase64 bool) {
	const textLine = "%d\t%s\n"
	for {
		entry := <-c
		for _, logEntry := range entry.Entries {
			if toBase64 {
				fmt.Printf(textLine, logEntry.Offset, base64.StdEncoding.EncodeToString(logEntry.Entry))
			} else {
				fmt.Printf(textLine, logEntry.Offset, string(logEntry.Entry))
			}
		}
		wg.Done()
	}
}

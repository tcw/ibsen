package client

import (
	"encoding/base64"
	"fmt"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/ext4"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

func ReadTopic(readPath string, toBase64 bool) {

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
			logChannel := make(chan *logStorage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			manger, err := ext4.NewBlockManger(dir, topic, 1024*1024*10)
			reader, err := ext4.NewTopicRead(&manger)
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			err = reader.ReadBatchFromOffsetNotIncluding(logStorage.ReadBatchParam{
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
			logChannel := make(chan *logStorage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			openFile, err := ext4.OpenFileForRead(readPath)
			err = ext4.ReadLogBlockFromOffsetNotIncluding(openFile, logStorage.ReadBatchParam{
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

func writeToStdOut(c chan *logStorage.LogEntryBatch, wg *sync.WaitGroup, toBase64 bool) {
	const textLine = "%d\t%s\n"
	for {
		entry := <-c
		for _, logEntry := range entry.Entries {
			if toBase64 {
				fmt.Printf(textLine, entry.Offset(), base64.StdEncoding.EncodeToString(logEntry.Entry))
			} else {
				fmt.Printf(textLine, entry.Offset(), string(logEntry.Entry))
			}
		}
		wg.Done()
	}
}

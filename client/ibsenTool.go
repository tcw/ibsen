package client

import (
	"encoding/base64"
	"fmt"
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
			log.Println("Absolute:", abs)
		}
		f, err := os.OpenFile(abs,
			os.O_RDONLY, 0400)
		if err != nil {
			log.Println(err)
			return
		}
		stat, err := f.Stat()
		if err != nil {
			log.Println(err)
			return
		}
		isDir := stat.IsDir()
		err = f.Close()
		if err != nil {
			log.Println(err)
			return
		}
		if isDir {
			dir, topic := path.Split(abs)
			logChannel := make(chan logStorage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			manger, err := ext4.NewBlockManger(dir, topic, 1024*1024*10)
			reader, err := ext4.NewTopicRead(&manger)
			if err != nil {
				log.Fatal(err)
			}
			err = reader.ReadBatchFromOffsetNotIncluding(logChannel, &wg, 1000, 0)
			if err != nil {
				log.Fatal(err)
			}
			wg.Wait()
		} else {
			logChannel := make(chan logStorage.LogEntryBatch)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg, toBase64)
			openFile, err := ext4.OpenFileForRead(readPath)
			err = ext4.ReadLogBlockFromOffsetNotIncluding(openFile, logChannel, &wg, 1000, 0)
			if err != nil {
				log.Println(err)
			}
			err = openFile.Close()
			if err != nil {
				println(err)
			}
			wg.Wait()
		}
	}
}

func writeToStdOut(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, toBase64 bool) {
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

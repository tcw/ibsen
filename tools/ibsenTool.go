package tools

import (
	"encoding/base64"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

func ReadTopic(readPath string) {

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
			logChannel := make(chan logStorage.LogEntry)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg)
			reader, err := ext4.NewTopicRead(dir, topic, 1024*1024*10)
			if err != nil {
				log.Fatal(err)
			}
			err = reader.ReadFromBeginning(logChannel, &wg)
			if err != nil {
				log.Fatal(err)
			}
			wg.Wait()
		} else {
			logChannel := make(chan logStorage.LogEntry)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg)
			openFile, err := ext4.OpenFileForRead(readPath)
			err = ext4.ReadLogToEnd(openFile, logChannel, &wg)
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

func writeToStdOut(c chan logStorage.LogEntry, wg *sync.WaitGroup) {
	for {
		entry := <-c
		base64Payload := base64.StdEncoding.EncodeToString(entry.Entry)
		fmt.Printf("%d\t%s\n", entry.Offset, base64Payload)
		wg.Done()
	}
}

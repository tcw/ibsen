package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

var (
	verbose = flag.Bool("v", false, "Verbose")
	topic   = flag.String("t", "", "Read log topic directory")
	file    = flag.String("f", "", "Read log file")
)

func main() {

	flag.Parse()
	if *topic != "" {
		abs, err := filepath.Abs(*topic)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		f, err := os.OpenFile(abs,
			os.O_RDONLY, 0400)
		if err != nil {
			fmt.Println(err)
			return
		}
		stat, err := f.Stat()
		if err != nil {
			fmt.Println(err)
			return
		}
		isDir := stat.IsDir()
		err = f.Close()
		if err != nil {
			fmt.Println(err)
			return
		}
		if isDir {
			dir, topic := path.Split(abs)
			logChannel := make(chan *logStorage.LogEntry)
			var wg sync.WaitGroup
			go writeToStdOut(logChannel, &wg)
			reader, err := ext4.NewTopicRead(dir, topic)
			if err != nil {
				log.Fatal(err)
			}
			err = reader.ReadFromBeginning(logChannel, &wg)
			if err != nil {
				log.Fatal(err)
			}
			wg.Wait()
		} else {
			fmt.Println("Not a dir")
		}

	}
	if *file != "" {
		abs, err := filepath.Abs(*file)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		logReader, err := ext4.NewLogReader(abs)
		if err != nil {
			log.Println(err)
			return
		}
		logChannel := make(chan *logStorage.LogEntry)
		var wg sync.WaitGroup
		go writeToStdOut(logChannel, &wg)
		err = logReader.ReadLogFromBeginning(logChannel, &wg)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		err = logReader.CloseLogReader()
		if err != nil {
			println(err)
		}
		wg.Wait()
	}

}

func writeToStdOut(c chan *logStorage.LogEntry, wg *sync.WaitGroup) {
	for {
		entry := <-c
		base64Payload := base64.StdEncoding.EncodeToString(entry.Entry)
		fmt.Printf("%d\t%d\t%s\n", entry.Offset, entry.ByteSize, base64Payload)
		wg.Done()
	}
}

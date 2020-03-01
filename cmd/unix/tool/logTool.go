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
			go writeToStdOut(logChannel)
			println(dir, topic)
			reader, err := ext4.NewTopicRead(dir, topic)
			if err != nil {
				log.Fatal(err)
			}
			err = reader.ReadFromBeginning(logChannel)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Println("Not a dir")
		}

	}
	if *file != "" {
		abs, err := filepath.Abs(*topic)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		reader := ext4.NewLogReader(abs)
		logChannel := make(chan *logStorage.LogEntry)
		go writeToStdOut(logChannel)
		err = reader.ReadLogFromBeginning(logChannel)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		reader.CloseLogReader()
	}

}

func writeToStdOut(c chan *logStorage.LogEntry) {
	for {
		entry := <-c
		base64Payload := base64.StdEncoding.EncodeToString(entry.Entry)
		fmt.Printf("%d\t%d\t%s\n", entry.Offset, entry.ByteSize, base64Payload)
	}
}

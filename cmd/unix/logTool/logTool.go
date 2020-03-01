package main

import (
	"dostoevsky/logSequencer"
	"dostoevsky/persistentLog"
	"encoding/base64"
	"flag"
	"fmt"
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
			logChannel := make(chan persistentLog.LogEntry)
			go writeToStdOut(logChannel)
			println(dir, topic)
			read := logSequencer.NewTopicRead(dir, topic)
			read.ReadFromBeginning(logChannel)
		} else {
			fmt.Println("Not a dir")
		}

	}
	if *file != "" {
		abs, err := filepath.Abs(*topic)
		if err != nil {
			fmt.Println("Absolute:", abs)
		}
		reader := persistentLog.NewLogReader(abs)
		logChannel := make(chan persistentLog.LogEntry)
		go writeToStdOut(logChannel)
		reader.ReadLogFromBeginning(logChannel)
		reader.CloseLogReader()
	}

}

func writeToStdOut(c chan persistentLog.LogEntry) {
	for {
		entry := <-c
		base64Payload := base64.StdEncoding.EncodeToString(entry.Payload)
		fmt.Printf("%d\t%d\t%s\n", entry.Offset, entry.Size, base64Payload)
	}
}

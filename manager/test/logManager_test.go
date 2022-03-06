package test

import (
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/manager"
	"log"
	"sync"
	"testing"
	"time"
)

func TestLogT(t *testing.T) {
	setUp()
	topicsManager, err := manager.NewLogTopicsManager(afs, 1*time.Second, 200*time.Millisecond, rootPath, 100)
	if err != nil {
		t.Error(err)
	}
	const testTopic = "cars"
	entries := createEntry(10, "hello", 0)
	_, err = topicsManager.Write(testTopic, entries)
	if err != nil {
		t.Error(err)
	}
	handler := topicsManager.Topics[testTopic]
	//go writeEvery100ms(handler, 1*time.Second, 100*time.Millisecond)
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go readVerification(t, logChan, &wg, "hello", 2)
	err = topicsManager.Read(access.ReadParams{
		Topic:     testTopic,
		Offset:    2,
		BatchSize: 1000,
		LogChan:   logChan,
		Wg:        &wg,
	})
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	log.Print(handler.Status())
	head := handler.IndexBlocks.Head()
	indexAccess := handler.LogIndexAccess
	read, err := indexAccess.Read(head.IndexFileName(topicsManager.RootPath, testTopic))
	log.Printf(read.ToString(), err)
}

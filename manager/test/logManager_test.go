package test

import (
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/manager"
	"sync"
	"testing"
	"time"
)

func TestLogT(t *testing.T) {
	setUp()
	const tenMB = 1024 * 1024
	topicsManager, err := manager.NewLogTopicsManager(afs, 2*time.Second, 100*time.Millisecond, rootPath, tenMB)
	if err != nil {
		t.Error(err)
	}
	const testTopic = "cars"
	_, err = topicsManager.Write(testTopic, createEntry(1000))
	if err != nil {
		t.Error(err)
	}
	go writeEvery100ms(topicsManager.Topics[testTopic], 1*time.Second, 10*time.Millisecond)

	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go readVerification(t, logChan, &wg)
	err = topicsManager.Read(access.ReadParams{
		Topic:     testTopic,
		Offset:    0,
		BatchSize: 1000,
		LogChan:   logChan,
		Wg:        &wg,
	})
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
}

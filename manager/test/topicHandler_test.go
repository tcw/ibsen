package test

import (
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/manager"
	"sync"
	"testing"
)

func TestTopicHandlerLoading(t *testing.T) {
	setUp()
	const tenMB = 1024 * 1024 * 10
	handler := manager.NewTopicHandler(afs, rootPath, "cars", tenMB)
	err := handler.Load()
	if err != nil {
		t.Error(err)
	}
}

//Todo: causes holes in block sequence
func TestTopicHandlerWriteRead(t *testing.T) {
	setUp()
	const tenMB = 1024 * 1024
	handler := manager.NewTopicHandler(afs, rootPath, "cars", tenMB)
	err := handler.Load()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 100; i++ {
		_, err = handler.Write(createEntry(10000))
		if err != nil {
			t.Error(err)
		}
	}
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go readVerification(t, logChan, &wg)
	_, err = handler.Read(access.ReadParams{
		Topic:     "cars",
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

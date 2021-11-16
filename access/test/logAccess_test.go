package test

import (
	"github.com/tcw/ibsen/access"
	"sync"
	"testing"
)

func TestCreateAndListTopic(t *testing.T) {
	setUp()
	testTopic := access.Topic("cars")
	err := logAccess.CreateTopic(testTopic)
	if err != nil {
		t.Error(err)
	}
	topics, err := logAccess.ListTopics()
	if err != nil {
		t.Error(err)
	}
	if topics[0] != testTopic {
		t.Fail()
	}
}

func TestWriteToTopic(t *testing.T) {
	setUp()
	var blockList []access.Block
	blockList = append(blockList, 0)
	blocks := access.Blocks{BlockList: blockList}
	testTopic := access.Topic("cars")

	entry := createEntry(2)
	fileName := blocks.Head().LogFileName(rootPath, testTopic)
	offset, bytes, err := logAccess.Write(fileName, entry, 0)
	if err != nil {
		t.Error(err)
	}
	if offset != 2 {
		t.Fail()
		t.Logf("expected %d actual %d", 2, offset)
	}
	if bytes < 12 {
		t.Fail()
		t.Logf("expected lagrer than %d actual %d", 12, bytes)
	}
	logBlocks, err := logAccess.ReadTopicLogBlocks(testTopic)
	if err != nil {
		t.Error(err)
	}
	if logBlocks.Size() != 1 {
		t.Fail()
		t.Logf("expected %d actual %d", 1, logBlocks.Size())
	}
	if logBlocks.Head() != 0 {
		t.Fail()
		t.Logf("expected  %d actual %d", 0, logBlocks.Head())
	}
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go readVerification(t, logChan, &wg)
	readOffset, err := logAccess.ReadLog(logBlocks.Head().LogFileName(rootPath, testTopic), access.ReadParams{
		Topic:     testTopic,
		Offset:    0,
		BatchSize: 10,
		LogChan:   logChan,
		Wg:        &wg,
	}, 0)

	wg.Wait()

	if err != nil {
		t.Error(err)
	}
	if readOffset != access.Offset(1) {
		t.Fail()
		t.Logf("expected %d actual %d", access.Offset(1), readOffset)
	}
}

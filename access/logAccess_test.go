package access

import (
	"github.com/spf13/afero"
	"log"
	"sync"
	"testing"
)

const rootPath = "/tmp/data"

var afs *afero.Afero
var logAccess LogAccess

func setUp() {
	var fs = afero.NewMemMapFs()
	afs = &afero.Afero{Fs: fs}
	logAccess = ReadWriteLogAccess{
		Afs:      afs,
		RootPath: rootPath,
	}
}

func TestCreateAndListTopic(t *testing.T) {
	setUp()
	testTopic := Topic("cars")
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
	var blockList []Block
	blockList = append(blockList, 0)
	blocks := Blocks{BlockList: blockList}
	testTopic := Topic("cars")

	entry := createEntry()
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
	logChan := make(chan *[]LogEntry)
	var wg sync.WaitGroup
	go readVerification(t, logChan, &wg)
	readOffset, err := logAccess.ReadLog(logBlocks.Head().LogFileName(rootPath, testTopic), ReadParams{
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
	if readOffset != Offset(1) {
		t.Fail()
		t.Logf("expected %d actual %d", Offset(1), readOffset)
	}
}

func readVerification(t *testing.T, logChan chan *[]LogEntry, wg *sync.WaitGroup) {
	entryBatch := <-logChan
	entries := *entryBatch
	if entries[0].Offset != 0 {
		t.Fail()
	}
	wg.Done()
	log.Println("2")
}

func createEntry() Entries {
	bytes := [][]byte{
		[]byte("hello3"),
		[]byte("hello4"),
	}
	return &bytes
}

package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"os"
	"sync"
	"testing"
)

const oneMB = 1024 * 1024 * 1
const testTopic1 = "testTopic1"
const testTopic2 = "testTopic2"

func newAfero() *afero.Afero {
	var fs = afero.NewMemMapFs()
	return &afero.Afero{Fs: fs}
}

func createTestDir(t *testing.T, afs *afero.Afero) string {
	testDir, err := afs.TempDir("", "ibsenTest")
	t.Log("created test dir: ", testDir)
	if err != nil {
		t.Error(err)
	}
	return testDir
}

func TestLogStorage_Create(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer afs.Fs.RemoveAll(dir)

	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(errore.SprintTrace(err))
	}
	if !create {
		t.Failed()
	}
}

func TestLogStorage_Drop(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer os.RemoveAll(dir)

	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Fail()
	}
	drop, err := storage.Drop(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !drop {
		t.Fail()
	}
	exists, _ := afs.Exists("." + testTopic1)
	if exists {
		t.Error("Topic has not been moved to . file")
		t.Fail()
	}
}

//Todo: create real status
func TestLogStorage_ListTopics(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Failed()
	}
	create2, err := storage.Create(testTopic2)
	if err != nil {
		t.Error(err)
	}
	if !create2 {
		t.Failed()
	}
	topics := storage.Status()
	if err != nil {
		t.Error(err)
	}
	t.Log(topics)
	if len(topics) != 2 {
		t.Fail()
	}
}

func TestLogStorage_WriteBatch_ReadBatch(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Failed()
	}

	for i := 0; i < 50000; i++ {
		n, err := storage.WriteBatch(&TopicBatchMessage{
			Topic: testTopic1,
			Message: [][]byte{
				[]byte("hello1"),
				[]byte("hello2"),
			},
		})
		if err != nil {
			t.Error(err)
		}
		if n == 0 {
			t.Fail()
		}
	}

	logChan := make(chan *LogEntryBatch)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadBatch(ReadBatchParam{
			LogChan:   logChan,
			Wg:        &wg,
			Topic:     testTopic1,
			BatchSize: 2,
			Offset:    0,
		})
		if err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < 50001; i = i + 2 {
		entry := <-logChan
		if entry.Entries[0].Offset != uint64(i) {
			t.Log("expected ", i, " actual ", entry.Entries[0].Offset)
			t.FailNow()
		}
	}
}

func TestLogStorage_ReadBatchFromOffsetNotIncluding(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Failed()
	}
	n, err := storage.WriteBatch(&TopicBatchMessage{
		Topic: testTopic1,
		Message: [][]byte{
			[]byte("hello1"),
			[]byte("hello2"),
		},
	})
	if err != nil {
		t.Error(err)
	}
	if n == 0 {
		t.Fail()
	}
	n, err = storage.WriteBatch(&TopicBatchMessage{
		Topic: testTopic1,
		Message: [][]byte{
			[]byte("hello3"),
			[]byte("hello4"),
		},
	})
	if err != nil {
		t.Error(err)
	}
	if n == 0 {
		t.Fail()
	}
	logChan := make(chan *LogEntryBatch)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadBatch(ReadBatchParam{
			LogChan:   logChan,
			Wg:        &wg,
			Topic:     testTopic1,
			BatchSize: 2,
			Offset:    1,
		})
		if err != nil {
			t.Error(err)
		}
	}()

	entry := <-logChan
	t.Log(entry)
	if entry.Entries[0].Offset != 2 {
		t.Fail()
	}
	if entry.Entries[1].Offset != 3 {
		t.Fail()
	}
}

func TestLogStorage_Corruption(t *testing.T) {
	afs := newAfero()
	dir := createTestDir(t, afs)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(afs, dir, oneMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic1)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Fail()
	}
	var bytes = []byte("hello1hello1hello1hello1hello1hello1hello1hello1")
	_, err = storage.WriteBatch(&TopicBatchMessage{
		Topic:   testTopic1,
		Message: [][]byte{bytes},
	})
	if err != nil {
		t.Error(err)
	}
	_, err = storage.WriteBatch(&TopicBatchMessage{
		Topic:   testTopic1,
		Message: [][]byte{bytes},
	})
	if err != nil {
		t.Error(err)
	}
	topics := storage.TopicManager.topics
	blockFileName, err := topics[testTopic1].currentBlockFileName()
	if err != nil {
		t.Error(err)
	}
	file, err := commons.OpenFileForReadWrite(afs, blockFileName)
	corruption, err := checkForCorruption(file)
	if err != nil {
		t.Error(err, corruption)
	}

	_, err = file.WriteAt([]byte("ooo"), 100)
	if err != nil {
		t.Error(err)
	}
	file.Sync()
	file.Close()
	file, err = commons.OpenFileForReadWrite(afs, blockFileName)
	corruption, err = checkForCorruption(file)
	if err == nil {
		t.Error("Did not detect corruption")
	}
	file.Close()
	err = correctFile(afs, blockFileName, corruption)
	if err != nil {
		t.Error(errore.SprintTrace(err))
	}
	file, err = commons.OpenFileForReadWrite(afs, blockFileName)
	corruption, err = checkForCorruption(file)
	if err != nil {
		t.Error(err, corruption)
	}
}

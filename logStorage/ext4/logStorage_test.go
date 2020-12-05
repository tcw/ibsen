package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

const oneMB = 1024 * 1024 * 1
const testTopic1 = "testTopic1"
const testTopic2 = "testTopic2"

func createTestDir(t *testing.T) string {
	testDir, err := ioutil.TempDir("", "ibsenTest")
	t.Log("created test dir: ", testDir)
	if err != nil {
		t.Error(err)
	}
	return testDir
}

func TestLogStorage_Create(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
}

func TestLogStorage_Drop(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
	directory := doesTopicExist(dir, "."+testTopic1)
	if !directory {
		t.Error("Topic has not been moved to . file")
		t.Fail()
	}
}

//Todo: create real status
func TestLogStorage_ListTopics(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
		n, err := storage.WriteBatch(&logStorage.TopicBatchMessage{
			Topic: testTopic1,
			Message: &[][]byte{
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

	logChan := make(chan *logStorage.LogEntryBatch)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadBatchFromOffsetNotIncluding(logChan, &wg, testTopic1, 2, 0)
		if err != nil {
			t.Error(err)
		}
	}()

	for i := 1; i < 50001; i = i + 2 {
		entry := <-logChan
		if entry.Entries[0].Offset != uint64(i) {
			t.Log("expected ", i, " actual ", entry.Entries[0].Offset)
			t.FailNow()
		}
	}
}

func TestLogStorage_ReadBatchFromOffsetNotIncluding(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
	n, err := storage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic: testTopic1,
		Message: &[][]byte{
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
	n, err = storage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic: testTopic1,
		Message: &[][]byte{
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
	logChan := make(chan *logStorage.LogEntryBatch)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadBatchFromOffsetNotIncluding(logChan, &wg, testTopic1, 2, 2)
		if err != nil {
			t.Error(err)
		}
	}()

	entry := <-logChan
	t.Log(entry)
	if entry.Entries[0].Offset != 3 {
		t.Fail()
	}
	if entry.Entries[1].Offset != 4 {
		t.Fail()
	}
}

func TestLogStorage_Corruption(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, oneMB)
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
	_, err = storage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic:   testTopic1,
		Message: &[][]byte{bytes},
	})
	if err != nil {
		t.Error(err)
	}
	_, err = storage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic:   testTopic1,
		Message: &[][]byte{bytes},
	})
	if err != nil {
		t.Error(err)
	}
	topics := storage.topicRegister.topics
	blockFileName := topics[testTopic1].CurrentBlockFileName()
	file, err := OpenFileForReadWrite(blockFileName)
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
	file, err = OpenFileForReadWrite(blockFileName)
	corruption, err = checkForCorruption(file)
	if err == nil {
		t.Error("Did not detect corruption")
	}
	file.Close()
	err = correctFile(blockFileName, corruption)
	if err != nil {
		t.Error(err)
	}
	file, err = OpenFileForReadWrite(blockFileName)
	corruption, err = checkForCorruption(file)
	if err != nil {
		t.Error(err, corruption)
	}
}

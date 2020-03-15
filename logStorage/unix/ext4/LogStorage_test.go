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
	topics, err := storage.ListTopics()
	if err != nil {
		t.Error(err)
	}
	t.Log(topics)
	if len(topics) != 2 {
		t.Fail()
	}
}

func TestLogStorage_Write_Read(t *testing.T) {
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
	n, err := storage.Write(&logStorage.TopicMessage{
		Topic:   testTopic1,
		Message: []byte("hello"),
	})
	if err != nil {
		t.Error(err)
	}
	if n == 0 {
		t.Fail()
	}
	logChan := make(chan *logStorage.LogEntry)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadFromBeginning(logChan, &wg, testTopic1)
		if err != nil {
			t.Error(err)
		}
	}()

	entry := <-logChan
	if entry.Offset != 1 {
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
	logChan := make(chan logStorage.LogEntryBatch)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadBatchFromBeginning(logChan, &wg, testTopic1, 2)
		if err != nil {
			t.Error(err)
		}
	}()

	entry := <-logChan
	if entry.Entries[0].Offset != 1 {
		t.Fail()
	}
	if entry.Entries[1].Offset != 2 {
		t.Fail()
	}
}

func TestLogStorage_ReadFromNotIncluding(t *testing.T) {
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
	n, err := storage.Write(&logStorage.TopicMessage{
		Topic:   testTopic1,
		Message: []byte("hello1"),
	})
	n, err = storage.Write(&logStorage.TopicMessage{
		Topic:   testTopic1,
		Message: []byte("hello2"),
	})
	if err != nil {
		t.Error(err)
	}
	if n == 0 {
		t.Fail()
	}
	logChan := make(chan *logStorage.LogEntry)
	var wg sync.WaitGroup

	go func() {
		err = storage.ReadFromNotIncluding(logChan, &wg, testTopic1, 1)
		if err != nil {
			t.Error(err)
		}
	}()

	entry := <-logChan
	if entry.Offset != 2 {
		t.Fail()
	}
	t.Log(entry)
}

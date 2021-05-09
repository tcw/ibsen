package index

import (
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"strconv"
	"testing"
	"time"
)

func TestTopicIndexManager_BuildIndex(t *testing.T) {
	const rootPath = "test"
	const topic = "topic1"
	const modulo = 5

	afs := newAfero()
	err := afs.Mkdir("test", 0755)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}

	manager, err := storage.NewTopicsManager(afs, rootPath, 1000)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	_, err = manager.CreateTopic(topic)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	topicManager := manager.GetTopicManager(topic)
	err = topicManager.WriteBatch(storage.CreateTestEntries(10, 100))
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}

	indexManager, err := NewTopicsIndexManager(afs, rootPath, &manager, modulo)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	indexManager.StartIndexing(500 * time.Millisecond)

	err = topicManager.WriteBatch(storage.CreateTestEntries(10, 100))
	err = topicManager.WriteBatch(storage.CreateTestEntries(10, 100))
	err = topicManager.WriteBatch(storage.CreateTestEntries(10000, 100))

	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	time.Sleep(2 * time.Second)
	indices, err := indexManager.TopicIndexManagers[topic].GetAllIndices()
	for u, idx := range indices {
		print(strconv.FormatUint(u, 10) + ":")
		for _, offset := range idx.ByteOffsets {
			print(offset)
			print(",")
		}
		println("")
	}

	/*
		Eventually(t,3,1*time.Second, func() bool {
			offset, err2 := indexManager.GetClosestByteOffset(topic, 7)
			if err2 != nil{
				fmt.Println(errore.SprintTrace(err2))
				return false
			}
			fmt.Println(offset.ByteOffset)
			return offset.ByteOffset == 1200
		})
	*/
}

func Eventually(t *testing.T, iterations int, interval time.Duration, f func() bool) {
	for i := 0; i < iterations; i++ {
		if assertTestFn(f) {
			return
		}
		time.Sleep(interval)
	}
	t.Fail()
}

func assertTestFn(f func() bool) (success bool) {
	defer func() {
		if e := recover(); e != nil {
			success = false
		}
	}()
	testResult := f()
	return testResult
}

package ext4

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"os"
	"sync"
)

func registerTopics(rootPath string, maxBlockSize int64) ([]*TopicWrite, error) {
	topics, err := ListTopics(rootPath)
	var topicWriters []*TopicWrite
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {

		topicWrite, err := NewTopicWrite(rootPath, topic, maxBlockSize)
		if err != nil {
			return nil, err
		}
		topicWriters = append(topicWriters, topicWrite)
	}
	return topicWriters, nil
}

type LogFile struct {
	LogWriter *bufio.Writer
	LogFile   *os.File
	FileName  string
}

type LogStorage struct {
	rootPath     string
	maxBlockSize int64
	wgTopic      sync.WaitGroup
	topicWriters map[string]*TopicWrite
	mu           sync.Mutex
}

func NewLogStorage(rootPath string, maxBlockSize int64) (*LogStorage, error) {
	storage := &LogStorage{
		rootPath:     rootPath,
		maxBlockSize: maxBlockSize,
		topicWriters: make(map[string]*TopicWrite),
	}

	storage.mu.Lock()
	topics, err := registerTopics(rootPath, maxBlockSize)
	for _, v := range topics {
		storage.topicWriters[v.name] = v
	}
	if err != nil {
		log.Fatal("Unable to register topics")
	}
	storage.mu.Unlock()
	return storage, nil
}

var _ logStorage.LogStorage = LogStorage{} // Verify that T implements I.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that *T implements I.

func (e LogStorage) Create(topic string) (bool, error) {
	e.mu.Lock()

	_, exists := e.topicWriters[topic]
	if exists {
		return false, nil
	}
	_, err := createTopic(e.rootPath, topic)
	if err != nil {
		return false, err
	}
	topicWrite, err2 := NewTopicWrite(e.rootPath, topic, e.maxBlockSize)
	if err2 != nil {
		return false, err2
	}

	e.topicWriters[topic] = topicWrite
	e.mu.Unlock()
	return true, nil
}

func (e LogStorage) Drop(topic string) (bool, error) {
	e.mu.Lock()
	_, exists := e.topicWriters[topic]
	if !exists {
		return false, errors.New(fmt.Sprintf("topic [%s] does not exits", topic))
	}
	delete(e.topicWriters, topic)
	oldLocation := e.rootPath + separator + topic
	newLocation := e.rootPath + separator + "." + topic
	err := os.Rename(oldLocation, newLocation)
	if err != nil {
		log.Fatal(err)
	}
	e.mu.Unlock()
	return true, nil
}

func (e LogStorage) ListTopics() ([]string, error) {
	directories, err := listUnhiddenDirectories(e.rootPath)
	if err != nil {
		return nil, err
	}
	return directories, nil
}

func (e LogStorage) Write(topicMessage *logStorage.TopicMessage) (int, error) {

	topicWriter := e.topicWriters[topicMessage.Topic]
	if topicWriter == nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s], topic not registered", topicMessage.Topic))
	}
	topicWriter.mu.Lock()
	n, err := topicWriter.WriteToTopic(topicMessage.Message)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s]", topicMessage.Topic))
	}
	topicWriter.mu.Unlock()
	return n, nil
}

func (e LogStorage) WriteBatch(topicMessage *logStorage.TopicBatchMessage) (int, error) {

	topicWriter := e.topicWriters[topicMessage.Topic]
	if topicWriter == nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s], topic not registered", topicMessage.Topic))
	}
	topicWriter.mu.Lock()
	n, err := topicWriter.WriteBatchToTopic(topicMessage.Message)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s]", topicMessage.Topic))
	}
	topicWriter.mu.Unlock()
	return n, nil
}

func (e LogStorage) ReadFromBeginning(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, topic string) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadFromBeginning(logChan, wg)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadFromNotIncluding(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, topic string, offset uint64) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadLogFromOffsetNotIncluding(logChan, wg, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadBatchFromBeginning(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, topic string, batchSize int) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadBatchFromBeginning(logChan, wg, batchSize)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, topic string, offset uint64, batchSize int) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadBatchFromOffsetNotIncluding(logChan, wg, offset, batchSize)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) Close() {
	for _, v := range e.topicWriters {
		err := v.Close()
		if err != nil {
			fmt.Println("unable to close writers cleanly")
		}
	}
}

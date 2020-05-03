package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type LogStorage struct {
	topicRegister *TopicRegister
}

func NewLogStorage(rootPath string, maxBlockSize int64) (LogStorage, error) {
	topics, err := newTopics(rootPath, maxBlockSize)
	if err != nil {
		return LogStorage{}, err
	}
	return LogStorage{&topics}, nil
}

var _ logStorage.LogStorage = LogStorage{} // Verify that T implements I.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that *T implements I.

func (e LogStorage) Create(topic string) (bool, error) {
	return e.topicRegister.CreateTopic(topic)

}

func (e LogStorage) Drop(topic string) (bool, error) {
	return e.topicRegister.CreateTopic(topic)
}

func (e LogStorage) ListTopics() ([]string, error) {
	return e.topicRegister.ListTopics()
}

func (e LogStorage) Write(topicMessage *logStorage.TopicMessage) (int, error) {
	registry := e.topicRegister.topics[topicMessage.Topic]
	err := registry.Write(topicMessage.Message)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (e LogStorage) WriteBatch(topicMessage *logStorage.TopicBatchMessage) (int, error) {
	registry := e.topicRegister.topics[topicMessage.Topic]
	err := registry.WriteBatch(topicMessage.Message)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (e LogStorage) ReadFromBeginning(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, topic string) error {
	read, err := NewTopicRead(e.topicRegister.topicsRootPath, topic, e.topicRegister.maxBlockSize)
	if err != nil {
		return err
	}
	err = read.ReadFromBeginning(logChan, wg)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadFromNotIncluding(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, topic string, offset uint64) error {
	read, err := NewTopicRead(e.topicRegister.topicsRootPath, topic, e.topicRegister.maxBlockSize)
	if err != nil {
		return err
	}
	err = read.ReadLogFromOffsetNotIncluding(logChan, wg, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadBatchFromBeginning(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, topic string, batchSize int) error {
	read, err := NewTopicRead(e.topicRegister.topicsRootPath, topic, e.topicRegister.maxBlockSize)
	if err != nil {
		return err
	}
	err = read.ReadBatchFromBeginning(logChan, wg, batchSize)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, topic string, offset uint64, batchSize int) error {
	read, err := NewTopicRead(e.topicRegister.topicsRootPath, topic, e.topicRegister.maxBlockSize)
	if err != nil {
		return err
	}
	err = read.ReadBatchFromOffsetNotIncluding(logChan, wg, offset, batchSize)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) Close() {
	//close
}

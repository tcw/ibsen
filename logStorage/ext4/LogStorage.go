package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type LogStorage struct {
	topicRegister *TopicManager
}

func NewLogStorage(rootPath string, maxBlockSize int64) (LogStorage, error) {
	topics, err := NewTopicManager(rootPath, maxBlockSize)
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
	return e.topicRegister.DropTopic(topic)
}

func (e LogStorage) Status() []*logStorage.TopicStatusMessage {
	topics := e.topicRegister.topics
	messages := make([]*logStorage.TopicStatusMessage, 0)
	for _, manager := range topics {
		messages = append(messages, &logStorage.TopicStatusMessage{
			Topic:        manager.topic,
			Blocks:       len(manager.blocks),
			Offset:       int64(manager.currentOffset),
			MaxBlockSize: manager.maxBlockSize,
			Path:         manager.rootPath,
		})
	}
	return messages
}

func (e LogStorage) WriteBatch(topicMessage *logStorage.TopicBatchMessage) (int, error) {
	registry := e.topicRegister.topics[topicMessage.Topic]
	err := registry.WriteBatch(topicMessage.Message)
	if err != nil {
		return 0, err
	}
	return len(*topicMessage.Message), nil
}

func (e LogStorage) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, topic string, batchSize int, offset uint64) error {
	read, err := NewTopicRead(e.topicRegister.topics[topic])
	if err != nil {
		return err
	}
	err = read.ReadBatchFromOffsetNotIncluding(logChan, wg, batchSize, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) Close() {
	//close
}

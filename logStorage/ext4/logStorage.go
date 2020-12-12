package ext4

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/logStorage"
)

type LogStorage struct {
	afs           *afero.Afero
	topicRegister *TopicManager
}

func NewLogStorage(afs *afero.Afero, rootPath string, maxBlockSize int64) (LogStorage, error) {
	topics, err := NewTopicManager(afs, rootPath, maxBlockSize)
	if err != nil {
		return LogStorage{}, errore.WrapWithContext(err)
	}
	return LogStorage{afs, &topics}, nil
}

var _ logStorage.LogStorage = LogStorage{} // Verify that interface is implemented.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that interface is implemented.

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
		return 0, errore.WrapWithContext(err)
	}
	return len(topicMessage.Message), nil
}

func (e LogStorage) ReadBatchFromOffsetNotIncluding(readBatchParam logStorage.ReadBatchParam) error {
	read, err := NewTopicRead(e.afs, e.topicRegister.topics[readBatchParam.Topic])
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = read.ReadBatchFromOffsetNotIncluding(readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (e LogStorage) Close() {
	//close
}

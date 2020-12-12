package logStorage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
)

type LogStorageAfero struct {
	afs           *afero.Afero
	topicRegister *TopicManager
}

func NewLogStorage(afs *afero.Afero, rootPath string, maxBlockSize int64) (LogStorageAfero, error) {
	topics, err := NewTopicManager(afs, rootPath, maxBlockSize)
	if err != nil {
		return LogStorageAfero{}, errore.WrapWithContext(err)
	}
	return LogStorageAfero{afs, &topics}, nil
}

var _ LogStorage = LogStorageAfero{} // Verify that interface is implemented.
//var _ logStorage.LogStorageAfero = (*LogStorageAfero{})(nil) // Verify that interface is implemented.

func (e LogStorageAfero) Create(topic string) (bool, error) {
	return e.topicRegister.CreateTopic(topic)
}

func (e LogStorageAfero) Drop(topic string) (bool, error) {
	return e.topicRegister.DropTopic(topic)
}

func (e LogStorageAfero) Status() []*TopicStatusMessage {
	topics := e.topicRegister.topics
	messages := make([]*TopicStatusMessage, 0)
	for _, manager := range topics {
		messages = append(messages, &TopicStatusMessage{
			Topic:        manager.topic,
			Blocks:       len(manager.blocks),
			Offset:       int64(manager.currentOffset),
			MaxBlockSize: manager.maxBlockSize,
			Path:         manager.rootPath,
		})
	}
	return messages
}

func (e LogStorageAfero) WriteBatch(topicMessage *TopicBatchMessage) (int, error) {
	registry := e.topicRegister.topics[topicMessage.Topic]
	err := registry.WriteBatch(topicMessage.Message)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return len(topicMessage.Message), nil
}

func (e LogStorageAfero) ReadBatchFromOffsetNotIncluding(readBatchParam ReadBatchParam) error {
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

func (e LogStorageAfero) Close() {
	//close
}

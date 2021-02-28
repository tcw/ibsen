package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/messaging"
	"time"
)

type LogStorageAfero struct {
	afs          *afero.Afero
	topicManager *TopicManager
}

func NewLogStorage(afs *afero.Afero, rootPath string, maxBlockSize int64) (LogStorageAfero, error) {
	topics, err := NewTopicManager(afs, rootPath, maxBlockSize)
	if err != nil {
		return LogStorageAfero{}, errore.WrapWithContext(err)
	}
	return LogStorageAfero{afs, &topics}, nil
}

var _ LogStorage = LogStorageAfero{} // Verify that interface is implemented.

func (e LogStorageAfero) Create(topic string) (bool, error) {
	return e.topicManager.CreateTopic(topic)
}

func (e LogStorageAfero) Drop(topic string) (bool, error) {
	return e.topicManager.DropTopic(topic)
}

func (e LogStorageAfero) Status() []*TopicStatusMessage {
	topics := e.topicManager.topics
	messages := make([]*TopicStatusMessage, 0)
	for _, manager := range topics {
		messages = append(messages, &TopicStatusMessage{
			Topic:        manager.topic,
			Blocks:       len(manager.blocks),
			Offset:       int64(manager.offset),
			MaxBlockSize: manager.maxBlockSize,
			Path:         manager.rootPath,
		})
	}
	return messages
}

func (e LogStorageAfero) WriteBatch(topicMessage *TopicBatchMessage) (int, error) {
	registry := e.topicManager.topics[topicMessage.Topic]
	if registry == nil {
		return 0, errore.NewWithContext("No such topic")
	}
	err := registry.WriteBatch(topicMessage.Message)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return len(topicMessage.Message), nil
}

func (e LogStorageAfero) ReadBatch(readBatchParam ReadBatchParam) error {
	blockManager := e.topicManager.topics[readBatchParam.Topic]
	if blockManager == nil {
		return nil
	}
	if readBatchParam.Offset == blockManager.offset {
		return nil
	}
	topicReader, err := NewTopicReader(e.afs, blockManager)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = topicReader.ReadFromOffset(readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (e LogStorageAfero) ReadStreamingBatch(readBatchParam ReadBatchParam) error {
	blockManager := e.topicManager.topics[readBatchParam.Topic]
	if blockManager == nil {
		return nil
	}
	if readBatchParam.Offset == blockManager.offset {
		return nil
	}
	topicReader, err := NewTopicReader(e.afs, blockManager)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	topicEventChannel := make(chan messaging.Event)
	messaging.Subscribe(blockManager.topic, topicEventChannel)
	err = topicReader.ReadFromOffset(readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for {
		select {
		//Todo: fix duplication
		case <-topicEventChannel:
			param := topicReader.NextReadBatchParam(readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
			err = topicReader.ReadFromOffset(param)
		case <-time.After(3 * time.Second):
			param := topicReader.NextReadBatchParam(readBatchParam.LogChan, readBatchParam.Wg, readBatchParam.BatchSize)
			err = topicReader.ReadFromOffset(param)
		}
	}
}

func (e LogStorageAfero) Close() {
	//close
}

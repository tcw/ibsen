package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/messaging"
	"time"
)

type LogStorageAfero struct {
	afs          *afero.Afero
	TopicManager *TopicsManager
}

func NewLogStorage(afs *afero.Afero, rootPath string, maxBlockSize int64) (LogStorageAfero, error) {
	topics, err := NewTopicsManager(afs, rootPath, maxBlockSize)
	if err != nil {
		return LogStorageAfero{}, errore.WrapWithContext(err)
	}
	return LogStorageAfero{afs, &topics}, nil
}

var _ LogStorage = LogStorageAfero{} // Verify that interface is implemented.

func (e LogStorageAfero) Create(topic string) (bool, error) {
	createTopic, err := e.TopicManager.CreateTopic(topic)
	if err != nil {
		return false, errore.WrapWithContext(err)
	}
	messaging.Publish(messaging.Event{
		Data: topic,
		Type: messaging.TopicCreatedEventType,
	})
	return createTopic, nil
}

func (e LogStorageAfero) Drop(topic string) (bool, error) {
	dropTopic, err := e.TopicManager.DropTopic(topic)
	if err != nil {
		return false, errore.WrapWithContext(err)
	}
	messaging.Publish(messaging.Event{
		Data: topic,
		Type: messaging.TopicDroppedEventType,
	})
	return dropTopic, nil
}

func (e LogStorageAfero) Status() []*TopicStatusMessage {
	topics := e.TopicManager.topics
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
	registry := e.TopicManager.topics[topicMessage.Topic]
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
	blockManager := e.TopicManager.topics[readBatchParam.Topic]
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
	_, _, err = topicReader.ReadFromOffset(readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (e LogStorageAfero) ReadStreamingBatch(readBatchParam ReadBatchParam) error {
	blockManager := e.TopicManager.topics[readBatchParam.Topic]
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
	messaging.Subscribe(topicEventChannel)
	var blockIndex = 0
	var internalOffset int64 = 0
	blockIndex, internalOffset, err = topicReader.ReadFromOffset(readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for {
		select {
		case <-topicEventChannel:
			blockIndex, internalOffset, err = topicReader.ReadFromInternalOffset(readBatchParam, blockIndex, internalOffset)
			if err != nil {
				return errore.WrapWithContext(err)
			}

		case <-time.After(3 * time.Second):
			blockIndex, internalOffset, err = topicReader.ReadFromInternalOffset(readBatchParam, blockIndex, internalOffset)
			if err != nil {
				return errore.WrapWithContext(err)
			}
		}
	}
}

func (e LogStorageAfero) Close() {
	//close
}

package manager

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"time"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (uint32, error)
	Read(params access.ReadParams) error
}

var _ LogManager = LogTopicsManager{}

type LogTopicsManager struct {
	Afs              *afero.Afero
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSize     uint64
	RootPath         string
	Topics           map[access.Topic]*TopicHandler
}

func NewLogTopicsManager(afs *afero.Afero, timeToLive time.Duration, checkForNewEvery time.Duration, rootPath string, maxBlockSizeMB uint64) (LogTopicsManager, error) {
	logAccess := access.ReadWriteLogAccess{
		Afs:      afs,
		RootPath: rootPath,
	}
	topics, err := logAccess.ListTopics()
	if err != nil {
		return LogTopicsManager{}, errore.WrapWithContext(err)
	}
	handlers := make(map[access.Topic]*TopicHandler)
	maxBlockSize := maxBlockSizeMB * 1024 * 1024
	for _, topic := range topics {
		handler := NewTopicHandler(afs, rootPath, topic, maxBlockSize)
		handlers[topic] = &handler
	}
	return LogTopicsManager{
		Afs:              afs,
		TTL:              timeToLive,
		CheckForNewEvery: checkForNewEvery,
		MaxBlockSize:     maxBlockSize,
		RootPath:         rootPath,
		Topics:           handlers,
	}, nil
}

func (l LogTopicsManager) Write(topic access.Topic, entries access.Entries) (uint32, error) {
	_, exists := l.Topics[topic]
	if !exists {
		l.addTopic(topic)
	}
	return l.Topics[topic].Write(entries)
}

func (l LogTopicsManager) Read(params access.ReadParams) error {
	_, exists := l.Topics[params.Topic]
	if !exists {
		return errors.New(fmt.Sprintf("Topic %s does not exits", params.Topic))
	}
	offset := params.Offset
	var err error
	readTTL := time.Now().Add(l.TTL)
	for time.Until(readTTL) > 0 {
		newParams := params
		newParams.Offset = offset
		offset, err = l.Topics[params.Topic].Read(newParams)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		time.Sleep(l.CheckForNewEvery)
	}
	return nil
}

func (l *LogTopicsManager) addTopic(topic access.Topic) {
	handler := NewTopicHandler(l.Afs, l.RootPath, topic, l.MaxBlockSize)
	l.Topics[topic] = &handler
}

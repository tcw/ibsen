package manager

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"time"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) error
	Read(params access.ReadParams) error
}

var _ LogManager = LogTopicsManager{}

type LogTopicsManager struct {
	Afs      *afero.Afero
	TTL      time.Duration
	RootPath string
	Topics   map[access.Topic]*TopicHandler
}

func newLogTopicsManager(afs *afero.Afero, timeToLive time.Duration, rootPath string) (LogTopicsManager, error) {
	logAccess := access.ReadWriteLogAccess{
		Afs:      afs,
		RootPath: rootPath,
	}
	topics, err := logAccess.ListTopics()
	if err != nil {
		return LogTopicsManager{}, errore.WrapWithContext(err)
	}
	var handlers map[access.Topic]*TopicHandler
	for _, topic := range topics {
		handler := newTopicHandler(afs, rootPath, topic)
		handlers[topic] = &handler
	}
	return LogTopicsManager{
		Afs:      afs,
		TTL:      timeToLive,
		RootPath: rootPath,
		Topics:   handlers,
	}, nil
}

func (l LogTopicsManager) Write(topic access.Topic, entries access.Entries) error {
	return l.Topics[topic].Write(entries)
}

func (l LogTopicsManager) Read(params access.ReadParams) error {
	offset, err := l.Topics[params.Topic].Read(params)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	readTTL := time.Now().Add(l.TTL)
	for time.Until(readTTL) > 0 {
		newParams := params
		params.Offset = offset
		offset, err = l.Topics[params.Topic].Read(newParams)
		time.Sleep(5 * time.Second)
	}
	return nil
}

func (l LogTopicsManager) addTopic() error {
	return nil
}

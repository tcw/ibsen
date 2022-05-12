package manager

import (
	"errors"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"sync"
	"time"
)

type TopicName string

type ReadParams struct {
	TopicName TopicName
	LogChan   chan *[]access.LogEntry
	Wg        *sync.WaitGroup
	From      access.Offset
	BatchSize uint32
}

type LogManager interface {
	List() []TopicName
	Write(topic TopicName, entries access.EntriesPtr) error
	Read(params ReadParams) error
}

var _ LogManager = &LogTopicsManager{}

type LogTopicsManager struct {
	ReadOnly         bool
	Afs              *afero.Afero
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSizeMB   int
	RootPath         string
	Topics           map[TopicName]*access.Topic
}

func NewLogTopicsManager(afs *afero.Afero, readonly bool, timeToLive time.Duration, checkForNewEvery time.Duration, rootPath string, maxBlockSizeMB int) (LogTopicsManager, error) {
	topics, err := access.ListAllTopics(afs, rootPath)
	if err != nil {
		return LogTopicsManager{}, errore.WrapWithContext(err)
	}
	topicMap := make(map[TopicName]*access.Topic)
	for _, topic := range topics {
		maxBlockSize := maxBlockSizeMB * 1024 * 1024
		logTopic := access.NewLogTopic(afs, rootPath, topic, maxBlockSize)
		topicMap[TopicName(topic)] = &logTopic
	}
	return LogTopicsManager{
		ReadOnly:         readonly,
		Afs:              afs,
		TTL:              timeToLive,
		CheckForNewEvery: checkForNewEvery,
		MaxBlockSizeMB:   maxBlockSizeMB,
		RootPath:         rootPath,
		Topics:           topicMap,
	}, nil
}

func (l *LogTopicsManager) List() []TopicName {
	return keys(l.Topics)
}

func (l *LogTopicsManager) Write(topicName TopicName, entries access.EntriesPtr) error {
	if l.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	_, exists := l.Topics[topicName]
	if !exists {
		err := access.CreateTopic(l.Afs, l.RootPath, string(topicName))
		if err != nil {
			return errore.WrapWithContext(err)
		}
		logTopic := access.NewLogTopic(l.Afs, l.RootPath, string(topicName), l.MaxBlockSizeMB)
		l.Topics[topicName] = &logTopic
	}
	return l.Topics[topicName].Write(entries)
}

func (l *LogTopicsManager) Read(params ReadParams) error {
	_, exists := l.Topics[params.TopicName]
	if !exists {
		return errore.NewWithContext("Topic %s does not exits", params.TopicName)
	}
	var err error
	readTTL := time.Now().Add(l.TTL)
	err = l.Topics[params.TopicName].Read(params.LogChan, params.Wg, params.From, params.BatchSize)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for time.Until(readTTL) > 0 {
		err = l.Topics[params.TopicName].Read(params.LogChan, params.Wg, params.From, params.BatchSize)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		time.Sleep(l.CheckForNewEvery)
	}
	return nil
}

func keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

package manager

import (
	"errors"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"log"
	"sync"
	"time"
)

type TopicName string

type ReadParams struct {
	TopicName        TopicName
	LogChan          chan *[]access.LogEntry
	Wg               *sync.WaitGroup
	From             access.Offset
	BatchSize        uint32
	StopOnCompletion bool
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
	Topics           map[TopicName]access.TopicAccess
}

func NewLogTopicsManager(afs *afero.Afero, readonly bool, timeToLive time.Duration, checkForNewEvery time.Duration, rootPath string, maxBlockSizeMB int) (LogTopicsManager, error) {
	maxBlockSize := maxBlockSizeMB * 1024 * 1024
	topics, err := access.ListAllTopics(afs, rootPath)
	if err != nil {
		return LogTopicsManager{}, errore.WrapWithContext(err)
	}
	topicMap := make(map[TopicName]access.TopicAccess)
	for _, topic := range topics {
		var iTopic access.TopicAccess
		iTopic = access.NewLogTopic(afs, rootPath, topic, maxBlockSize, false)
		topicMap[TopicName(topic)] = iTopic
	}
	return LogTopicsManager{
		ReadOnly:         readonly,
		Afs:              afs,
		TTL:              timeToLive,
		CheckForNewEvery: checkForNewEvery,
		MaxBlockSizeMB:   maxBlockSize,
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
	topic, exists := l.Topics[topicName]
	if !exists {
		err := access.CreateTopic(l.Afs, l.RootPath, string(topicName))
		if err != nil {
			return errore.WrapWithContext(err)
		}
		topic = access.NewLogTopic(l.Afs, l.RootPath, string(topicName), l.MaxBlockSizeMB, true)
		l.Topics[topicName] = topic
	}
	if !topic.IsLoaded() {
		err := topic.Load()
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	return topic.Write(entries)
}

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic, exists := l.Topics[params.TopicName]
	if !exists {
		return errore.NewWithContext("Topic %s does not exits", params.TopicName)
	}
	if !topic.IsLoaded() {
		err := topic.Load()
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	var err error
	readTTL := time.Now().Add(l.TTL)
	err = topic.Read(params.LogChan, params.Wg, params.From, params.BatchSize)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if params.StopOnCompletion {
		return nil
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

func (l *LogTopicsManager) indexScheduler() {
	for {
		for name, topic := range l.Topics {
			err := topic.UpdateIndex()
			if err != nil {
				log.Printf("index builder for topic %s has failed: %s", name,
					errore.SprintTrace(errore.WrapWithContext(err)))
			}
		}
		time.Sleep(time.Second * 10)
	}
}

func keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

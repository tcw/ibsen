package manager

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"sync"
	"time"
)

type TopicName string

type ReadParams struct {
	TopicName          TopicName
	LogChan            chan *[]access.LogEntry
	Wg                 *sync.WaitGroup
	From               access.Offset
	BatchSize          uint32
	ReturnOnCompletion bool
}

type LogManager interface {
	List() []TopicName
	Write(topic TopicName, entries access.EntriesPtr) error
	Read(params ReadParams) error
}

var _ LogManager = &LogTopicsManager{}

type LogTopicManagerParams struct {
	ReadOnly         bool
	Afs              *afero.Afero
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSize     int
	RootPath         string
}

type LogTopicsManager struct {
	Params           LogTopicManagerParams
	Topics           *sync.Map
	TopicWriteLocker *sync.Map
	LoadedTopics     *sync.Map
}

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	return LogTopicsManager{
		Params:           params,
		Topics:           &sync.Map{},
		TopicWriteLocker: &sync.Map{},
		LoadedTopics:     &sync.Map{},
	}, nil
}

func (l *LogTopicsManager) List() []TopicName {
	topics, err := access.ListAllTopics(l.Params.Afs, l.Params.RootPath)
	if err != nil {
		log.Err(err).Msg("failed listing topics")
	}
	var topicNames []TopicName
	for _, topic := range topics {
		topicNames = append(topicNames, TopicName(topic))
	}
	return topicNames
}

func (l *LogTopicsManager) Write(topicName TopicName, entries access.EntriesPtr) error {
	if l.Params.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	locker, _ := l.TopicWriteLocker.LoadOrStore(string(topicName), &sync.Mutex{})
	var mutex = locker.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	topic := l.getTopic(topicName)
	return topic.Write(entries)
}

var TopicNotFound error = errors.New("topic not found")

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic := l.getTopic(params.TopicName)
	readFrom := params.From
	return topic.ReadLog(access.ReadLogParams{
		LogChan:   params.LogChan,
		Wg:        params.Wg,
		From:      readFrom,
		BatchSize: params.BatchSize,
	})
}

func (l *LogTopicsManager) allTopics() map[string]*access.Topic {
	var topics map[string]*access.Topic
	l.Topics.Range(func(key, value any) bool {
		topics[key.(string)] = value.(*access.Topic)
		return true
	})
	return topics
}

func (l *LogTopicsManager) getTopic(name TopicName) *access.Topic {
	_, loaded := l.LoadedTopics.LoadOrStore(string(name), "dummy")
	if !loaded {
		l.Topics.Store(string(name), l.newTopic(name))
	}
	value, _ := l.Topics.Load(string(name))
	return value.(*access.Topic)
}

func (l *LogTopicsManager) newTopic(topicName TopicName) *access.Topic {
	createdTopicDirectory, err := access.CreateTopicDirectory(l.Params.Afs, l.Params.RootPath, string(topicName))
	if err != nil {
		log.Fatal().Str("topic", string(topicName)).
			Str("stack", errore.SprintStackTraceBd(err)).
			Err(err).
			Msg("unable to create new topic directory")
	}
	topic := access.NewLogTopic(access.TopicParams{
		Afs:          l.Params.Afs,
		RootPath:     l.Params.RootPath,
		TopicName:    string(topicName),
		MaxBlockSize: l.Params.MaxBlockSize,
	})
	if !createdTopicDirectory {
		log.Info().Str("topic", string(topicName)).Msg("loaded")
		err = topic.Load()
		if err != nil {
			log.Fatal().Str("topic", string(topicName)).
				Str("stack", errore.SprintStackTraceBd(err)).
				Err(err).
				Msg("unable to load topic")
		}
	}
	log.Info().Str("topic", string(topicName)).Msg("created")
	return topic
}

func (l *LogTopicsManager) indexScheduler() {
	for {
		for name, topic := range l.allTopics() {
			_, err := topic.UpdateIndex()
			if err != nil {
				log.Err(err).Msg(fmt.Sprintf("index builder for topic %s has failed", name))
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

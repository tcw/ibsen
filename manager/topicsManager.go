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

type LogTopicManagerParams struct {
	ReadOnly         bool
	Afs              *afero.Afero
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSize     int
	RootPath         string
}

type LogTopicsManager struct {
	Params             LogTopicManagerParams
	TopicWriteLocker   *sync.Map
	Topics             *sync.Map
	TerminationChannel chan bool
}

var TopicNotFound = errors.New("topic not found")

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	manager := LogTopicsManager{
		Params:             params,
		TopicWriteLocker:   &sync.Map{},
		Topics:             &sync.Map{},
		TerminationChannel: make(chan bool),
	}
	go manager.startIndexScheduler(manager.TerminationChannel)
	return manager, nil
}

func (l *LogTopicsManager) ShutdownIndexer() {
	l.TerminationChannel <- true
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
	topic := l.getOrCreateTopic(topicName)
	locker, _ := l.TopicWriteLocker.LoadOrStore(string(topicName), &sync.Mutex{})
	var mutex = locker.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	return topic.Write(entries)
}

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic := l.getOrCreateTopic(params.TopicName)
	readFrom := params.From
	return topic.Read(access.ReadLogParams{
		LogChan:   params.LogChan,
		Wg:        params.Wg,
		From:      readFrom,
		BatchSize: params.BatchSize,
	})
}

func (l *LogTopicsManager) getOrCreateTopic(name TopicName) *access.Topic {
	topic, ok := l.Topics.Load(string(name))
	if !ok {
		topic = l.loadOrCreateNewTopic(name)
		topic, _ = l.Topics.LoadOrStore(string(name), topic)
	}
	return topic.(*access.Topic)
}

func (l *LogTopicsManager) loadOrCreateNewTopic(topicName TopicName) *access.Topic {
	created, err := access.CreateTopicDirectory(l.Params.Afs, l.Params.RootPath, string(topicName))
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
	if !created {
		err = topic.Load()
		if err == access.NoBlocksFound {
			log.Err(err).Str("topic", string(topicName)).
				Msg("Topic was not loaded nor created")
		}
		if err != nil {
			log.Fatal().Str("topic", string(topicName)).
				Str("stack", errore.SprintStackTraceBd(err)).
				Err(err).
				Msg("unable to load topic")
		}
		log.Info().Str("topic", string(topicName)).Msg("loaded topic from disk")
	} else {
		log.Info().Str("topic", string(topicName)).Msg("created topic")
	}
	return topic
}

func (l *LogTopicsManager) startIndexScheduler(terminate chan bool) {
	for {
		select {
		case <-terminate:
			close(terminate)
			return
		default:
			time.Sleep(time.Second * 10)
			l.Topics.Range(func(key, value any) bool {
				_, err := value.(*access.Topic).UpdateIndex()
				if err != nil {
					log.Err(err).Msg(fmt.Sprintf("index builder for topic %s has failed", key.(string)))
				}
				return true
			})
		}
	}
}

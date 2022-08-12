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
	Params LogTopicManagerParams
	Topics map[TopicName]access.TopicAccess
}

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	topics, err := access.ListAllTopics(params.Afs, params.RootPath)
	if err != nil {
		return LogTopicsManager{}, err
	}
	topicMap := make(map[TopicName]access.TopicAccess)
	for _, topic := range topics {
		var iTopic access.TopicAccess
		iTopic = access.NewLogTopic(access.TopicParams{
			Afs:          params.Afs,
			RootPath:     params.RootPath,
			TopicName:    topic,
			MaxBlockSize: params.MaxBlockSize,
			Loaded:       false,
		})
		topicMap[TopicName(topic)] = iTopic
	}
	return LogTopicsManager{
		Params: params,
		Topics: topicMap,
	}, nil
}

func (l *LogTopicsManager) List() []TopicName {
	return keys(l.Topics)
}

func (l *LogTopicsManager) Write(topicName TopicName, entries access.EntriesPtr) error {
	if l.Params.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	topic, exists := l.Topics[topicName]
	if !exists {
		err := access.CreateTopic(l.Params.Afs, l.Params.RootPath, string(topicName))
		if err != nil {
			return errore.Wrap(err)
		}
		topic = access.NewLogTopic(access.TopicParams{
			Afs:          l.Params.Afs,
			RootPath:     l.Params.RootPath,
			TopicName:    string(topicName),
			MaxBlockSize: l.Params.MaxBlockSize,
			Loaded:       true,
		})
		l.Topics[topicName] = topic
	}
	if !topic.IsLoaded() {
		err := topic.Load()
		if err != nil {
			return errore.Wrap(err)
		}
	}
	return topic.Write(entries)
}

var TopicNotFound error = errors.New("topic not found")

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic, exists := l.Topics[params.TopicName]
	if !exists {
		return TopicNotFound
	}
	if !topic.IsLoaded() {
		err := topic.Load()
		if err != nil {
			return errore.Wrap(err)
		}
	}
	readTTL := time.Now().Add(l.Params.TTL)
	readFrom := params.From
	for time.Until(readTTL) > 0 {
		log.Debug().
			Uint64("from", uint64(readFrom)).
			Msg("read manager")
		readResult, err := topic.ReadLog(access.ReadLogParams{
			LogChan:   params.LogChan,
			Wg:        params.Wg,
			From:      readFrom,
			BatchSize: params.BatchSize,
		})
		log.Debug().Uint64("lastOffset", uint64(readResult.LastLogOffset)).
			Uint64("entries", readResult.EntriesRead).
			Msg("read manager result")
		if err == access.NoEntriesFound {
			time.Sleep(l.Params.CheckForNewEvery)
			continue
		}
		if err != nil {
			return errore.Wrap(err)
		}
		if params.ReturnOnCompletion {
			return nil
		}
		if readResult.EntriesRead > 0 {
			readTTL = time.Now().Add(l.Params.TTL)
			readFrom = readResult.NextOffset()
		}
		time.Sleep(l.Params.CheckForNewEvery)
	}
	return nil
}

func (l *LogTopicsManager) indexScheduler() {
	for {
		for name, topic := range l.Topics {
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

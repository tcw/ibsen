package manager

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
	"sync"
	"time"
)

type ReadParams struct {
	TopicName common.TopicName
	LogChan   chan *[]common.LogEntry
	Wg        *sync.WaitGroup
	From      common.Offset
	BatchSize uint32
	// Cancel stops the read when closed; nil never cancels.
	Cancel <-chan struct{}
}

type LogManager interface {
	List() []common.TopicName
	Write(topic common.TopicName, entries common.EntriesPtr) error
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
	StatusAccess       access.StatusAccess
}

var TopicNotFound = errors.New("topic not found")

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	manager := LogTopicsManager{
		Params:             params,
		TopicWriteLocker:   &sync.Map{},
		Topics:             &sync.Map{},
		TerminationChannel: make(chan bool),
		StatusAccess: &access.Status{
			Afs:      params.Afs,
			RootPath: params.RootPath,
		},
	}
	go manager.startIndexScheduler(manager.TerminationChannel)
	return manager, nil
}

func (l *LogTopicsManager) ShutdownIndexer() {
	l.TerminationChannel <- true
}

func (l *LogTopicsManager) List() []common.TopicName {
	return l.StatusAccess.List()
}

func (l *LogTopicsManager) Write(topicName common.TopicName, entries common.EntriesPtr) error {
	if l.Params.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	topic, err := l.getOrCreateTopic(topicName)
	if err != nil {
		return err
	}
	locker, _ := l.TopicWriteLocker.LoadOrStore(string(topicName), &sync.Mutex{})
	var mutex = locker.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	return topic.Write(entries)
}

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic, err := l.getOrCreateTopic(params.TopicName)
	if err != nil {
		return err
	}
	readFrom := params.From
	return topic.Read(common.ReadLogParams{
		LogChan:   params.LogChan,
		Wg:        params.Wg,
		From:      readFrom,
		BatchSize: params.BatchSize,
		Cancel:    params.Cancel,
	})
}

func (l *LogTopicsManager) getOrCreateTopic(name common.TopicName) (*access.Topic, error) {
	topic, ok := l.Topics.Load(string(name))
	if !ok {
		loaded, err := l.loadOrCreateNewTopic(name)
		if err != nil {
			return nil, err
		}
		topic, _ = l.Topics.LoadOrStore(string(name), loaded)
	}
	return topic.(*access.Topic), nil
}

// loadOrCreateNewTopic loads a topic from disk or creates it. A topic that fails to load is
// not cached, so its requests keep failing until the problem is fixed while other topics
// keep working.
func (l *LogTopicsManager) loadOrCreateNewTopic(topicName common.TopicName) (*access.Topic, error) {
	topic := access.NewLogTopic(common.TopicParams{
		Afs:          l.Params.Afs,
		RootPath:     l.Params.RootPath,
		TopicName:    string(topicName),
		MaxBlockSize: l.Params.MaxBlockSize,
	})
	if err := topic.LoadOrCreate(); err != nil {
		return nil, errore.WrapWithContextF(err, "unable to load topic %s", topicName)
	}
	return topic, nil
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

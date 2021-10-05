package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/messaging"
	"github.com/tcw/ibsen/storage"
	"log"
	"sync"
	"time"
)

type FixedIntervalIndexManager struct {
	afs                 *afero.Afero
	rootPath            string
	modulo              uint32
	topicsManager       *storage.TopicsManager
	TopicIndexManagers  map[string]*TopicIndexManager
	mu                  *sync.Mutex
	indexWatcherStarted bool
}

type IbsenIndex interface {
	StartIndexing(updateEvery time.Duration)
	GetClosestByteOffset(topic string, offset access.Offset) (commons.IndexedOffset, error)
	RebuildIndex(topic string) error
	CreateIndex(topic string) error
	DropIndex(topic string) error
}

var _ IbsenIndex = (*FixedIntervalIndexManager)(nil) //Verify that interface is implemented

func NewTopicsIndexManager(afs *afero.Afero, rootPath string, topicsManager *storage.TopicsManager, modulo uint32) (*FixedIntervalIndexManager, error) {
	topics, err := commons.ListUnhiddenEntriesDirectory(afs, rootPath)
	if err != nil {
		return nil, err
	}
	var topicIndexManagers = make(map[string]*TopicIndexManager)
	for _, topic := range topics {
		manager, err := NewTopicIndexManager(TopicIndexParams{
			afs:          afs,
			topic:        topic,
			rootPath:     rootPath,
			topicManager: topicsManager.GetTopicManager(topic),
			modulo:       modulo,
		})
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		topicIndexManagers[topic] = manager
	}
	return &FixedIntervalIndexManager{
		afs:                 afs,
		rootPath:            rootPath,
		modulo:              modulo,
		topicsManager:       topicsManager,
		TopicIndexManagers:  topicIndexManagers,
		mu:                  &sync.Mutex{},
		indexWatcherStarted: false,
	}, err
}

func (tim *FixedIntervalIndexManager) StartIndexing(updateEvery time.Duration) {
	tim.mu.Lock()
	defer tim.mu.Unlock()
	if tim.indexWatcherStarted {
		return
	}
	tim.indexWatcherStarted = true
	topicEventChannel := make(chan messaging.Event)
	messaging.Subscribe(topicEventChannel)
	go startIndexWatcher(topicEventChannel, tim, updateEvery)
}

func (tim *FixedIntervalIndexManager) GetClosestByteOffset(topic string, offset access.Offset) (commons.IndexedOffset, error) {
	index, err := tim.TopicIndexManagers[topic].FindClosestIndex(offset)
	if err != nil {
		return commons.IndexedOffset{}, errore.WrapWithContext(err)
	}
	return index, nil
}

func (tim *FixedIntervalIndexManager) CreateIndex(topic string) error {
	manager, err := NewTopicIndexManager(TopicIndexParams{
		afs:          tim.afs,
		topic:        topic,
		rootPath:     tim.rootPath,
		topicManager: tim.topicsManager.GetTopicManager(topic),
		modulo:       tim.modulo,
	})
	if err != nil {
		return errore.WrapWithContext(err)
	}
	tim.TopicIndexManagers[topic] = manager
	return nil
}

func (tim *FixedIntervalIndexManager) DropIndex(topic string) error {
	index, err := tim.TopicIndexManagers[topic].DropIndex()
	log.Printf("Dropped %d index blocks\n", index)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	tim.TopicIndexManagers[topic] = nil //todo: check if this creates trouble down the line
	return nil
}

func (tim *FixedIntervalIndexManager) RebuildIndex(topic string) error {
	index, err := tim.TopicIndexManagers[topic].DropIndex()
	log.Printf("Dropped %d index blocks for rebuild\n", index)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = tim.TopicIndexManagers[topic].BuildIndex()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func startIndexWatcher(topicEventChannel chan messaging.Event, tim *FixedIntervalIndexManager, updateEvery time.Duration) {
	for {
		select {
		case event := <-topicEventChannel:
			if event.Type == messaging.TopicChangeEventType {
				topicChange := event.Data.(messaging.TopicChange)
				err := tim.TopicIndexManagers[topicChange.Topic].BuildIndex()
				if err != nil {
					log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			}
		case <-time.After(updateEvery):
			for s, manager := range tim.TopicIndexManagers {
				err := manager.BuildIndex()
				if err != nil {
					log.Printf("Update of index %s failed", s)
					log.Println(errore.SprintTrace(err))
				}
			}
		}
	}
}

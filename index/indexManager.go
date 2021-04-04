package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/messaging"
	"github.com/tcw/ibsen/storage"
	"log"
	"time"
)

type TopicsIndexManager struct {
	afs                *afero.Afero
	rootPath           string
	modulo             uint32
	topicsManager      *storage.TopicsManager
	TopicIndexManagers map[string]*TopicIndexManager
}

func NewTopicsIndexManager(afs *afero.Afero, rootPath string, topicsManager *storage.TopicsManager, modulo uint32) (*TopicsIndexManager, error) {
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
			topicManager: topicsManager.GetBlockManager(topic),
			modulo:       modulo,
		})
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		topicIndexManagers[topic] = manager
	}
	return &TopicsIndexManager{
		afs:                afs,
		rootPath:           rootPath,
		modulo:             modulo,
		topicsManager:      topicsManager,
		TopicIndexManagers: topicIndexManagers,
	}, err
}

func (tim *TopicsIndexManager) StartIndexing() {

	topicEventChannel := make(chan messaging.Event)
	messaging.SubscribeAll(topicEventChannel)

	go startIndexWatcher(topicEventChannel, tim)

}

func startIndexWatcher(topicEventChannel chan messaging.Event, tim *TopicsIndexManager) {
	for {
		select {
		case event := <-topicEventChannel:
			if event.Type == messaging.TopicChangeEventType {
				topicChange := event.Data.(messaging.TopicChange)
				err := tim.TopicIndexManagers[topicChange.Topic].BuildIndex()
				if err != nil {
					log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			} else if event.Type == messaging.TopicCreatedEventType {
				manager, err := NewTopicIndexManager(TopicIndexParams{
					afs:          tim.afs,
					topic:        event.Data.(string),
					rootPath:     tim.rootPath,
					topicManager: tim.topicsManager.GetBlockManager(event.Data.(string)),
					modulo:       tim.modulo,
				})
				if err != nil {
					log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
				}
				tim.TopicIndexManagers[event.Data.(string)] = manager
			}
		case <-time.After(10 * time.Second):
			log.Printf("index check")
		}
	}
}

func (tim *TopicsIndexManager) BuildIndex(topic string) error {
	err := tim.TopicIndexManagers[topic].BuildIndex()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

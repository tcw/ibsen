package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
)

type TopicsIndexManager struct {
	TopicIndexManagers map[string]*TopicIndexManager
}

func NewTopicsIndexManager(afs *afero.Afero, rootPath string, topicManager *storage.TopicManager, modulo uint32) (*TopicsIndexManager, error) {
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
			topicManager: topicManager,
			modulo:       modulo,
		})
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		topicIndexManagers[topic] = manager
	}
	return &TopicsIndexManager{TopicIndexManagers: topicIndexManagers}, err
}

package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"log"
)

type TopicsManager struct {
	afs            *afero.Afero
	topicsRootPath string
	topics         map[string]*TopicManager
	maxBlockSize   int64
}

func NewTopicManager(afs *afero.Afero, rootPath string, maxBlockSize int64) (TopicsManager, error) {

	var topics = map[string]*TopicManager{}
	topicManager := TopicsManager{
		afs:            afs,
		maxBlockSize:   maxBlockSize,
		topicsRootPath: rootPath,
		topics:         topics,
	}
	err := topicManager.UpdateTopicsFromStorage()
	if err != nil {
		return TopicsManager{}, errore.WrapWithContext(err)
	}
	return topicManager, nil
}

func (tr *TopicsManager) GetBlockManager(topic string) *TopicManager {
	return tr.topics[topic]
}

func (tr *TopicsManager) UpdateTopicsFromStorage() error {
	directories, err := commons.ListUnhiddenEntriesDirectory(tr.afs, tr.topicsRootPath)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	managerChan := make(chan TopicManager)
	for _, topic := range directories {
		go func(topic string) {
			blockManger, err := NewBlockManger(tr.afs, tr.topicsRootPath, topic, tr.maxBlockSize)
			if err != nil {
				log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
			}
			managerChan <- blockManger
		}(topic)
	}
	for range directories {
		manager := <-managerChan
		topic := manager.topic
		tr.topics[topic] = &manager
	}
	return nil
}

func (tr *TopicsManager) CreateTopic(topic string) (bool, error) {
	if tr.doesTopicExist(topic) {
		return false, nil
	}
	err := tr.afs.Mkdir(tr.topicsRootPath+commons.Separator+topic, 0777) //Todo: more restrictive
	if err != nil {
		return false, errore.WrapWithContext(err)
	}
	registry, err := NewBlockManger(tr.afs, tr.topicsRootPath, topic, tr.maxBlockSize)
	if err != nil {
		return false, errore.WrapWithContext(err)
	}

	tr.topics[topic] = &registry
	return true, nil
}

func (tr *TopicsManager) DropTopic(topic string) (bool, error) {

	if !tr.doesTopicExist(topic) {
		return false, nil
	}
	oldLocation := tr.topicsRootPath + commons.Separator + topic
	newLocation := tr.topicsRootPath + commons.Separator + "." + topic
	err := tr.afs.Rename(oldLocation, newLocation)
	if err != nil {
		return false, errore.WrapWithContext(err)
	}
	delete(tr.topics, topic)
	return true, nil
}

func (tr *TopicsManager) ListTopics() ([]string, error) {
	topics := make([]string, 0, len(tr.topics))
	for k := range tr.topics {
		topics = append(topics, k)
	}
	return topics, nil
}

func (tr *TopicsManager) doesTopicExist(topic string) bool {
	_, exists := tr.topics[topic]
	return exists
}

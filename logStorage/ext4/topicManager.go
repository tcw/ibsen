package ext4

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
)

type TopicManager struct {
	afs            *afero.Afero
	topicsRootPath string
	topics         map[string]*BlockManager
	maxBlockSize   int64
}

func NewTopicManager(afs *afero.Afero, rootPath string, maxBlockSize int64) (TopicManager, error) {

	var topics = map[string]*BlockManager{}
	register := TopicManager{
		afs:            afs,
		maxBlockSize:   maxBlockSize,
		topicsRootPath: rootPath,
		topics:         topics,
	}
	err := register.UpdateTopicsFromStorage()
	if err != nil {
		return TopicManager{}, errore.WrapWithContext(err)
	}
	return register, nil
}

func (tr *TopicManager) UpdateTopicsFromStorage() error {
	directories, err := listUnhiddenDirectories(tr.afs, tr.topicsRootPath)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for _, topic := range directories {
		registry, err := NewBlockManger(tr.afs, tr.topicsRootPath, topic, tr.maxBlockSize)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		tr.topics[topic] = &registry
	}
	return nil
}

func (tr *TopicManager) CreateTopic(topic string) (bool, error) {
	if tr.doesTopicExist(topic) {
		return false, nil
	}
	err := tr.afs.Mkdir(tr.topicsRootPath+separator+topic, 0777) //Todo: more restrictive
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

func (tr *TopicManager) DropTopic(topic string) (bool, error) {

	if !tr.doesTopicExist(topic) {
		return false, nil
	}
	oldLocation := tr.topicsRootPath + separator + topic
	newLocation := tr.topicsRootPath + separator + "." + topic
	err := tr.afs.Rename(oldLocation, newLocation)
	if err != nil {
		return false, errore.WrapWithContext(err)
	}
	delete(tr.topics, topic)
	return true, nil
}

func (tr *TopicManager) ListTopics() ([]string, error) {
	topics := make([]string, 0, len(tr.topics))
	for k := range tr.topics {
		topics = append(topics, k)
	}
	return topics, nil
}

func (tr *TopicManager) doesTopicExist(topic string) bool {
	_, exists := tr.topics[topic]
	return exists
}

package ext4

import (
	"os"
)

type TopicRegister struct {
	topicsRootPath string
	topics         map[string]*BlockRegistry
	maxBlockSize   int64
}

func newTopics(rootPath string, maxBlockSize int64) (TopicRegister, error) {
	var topics = map[string]*BlockRegistry{}
	register := TopicRegister{
		maxBlockSize:   maxBlockSize,
		topicsRootPath: rootPath,
		topics:         topics,
	}
	err := register.UpdateTopicsFromStorage()
	if err != nil {
		return TopicRegister{}, err
	}
	return register, nil
}

func (tr *TopicRegister) UpdateTopicsFromStorage() error {
	directories, err := listUnhiddenDirectories(tr.topicsRootPath)
	if err != nil {
		return err
	}
	for _, topic := range directories {
		registry, err := NewBlockRegistry(tr.topicsRootPath, topic, tr.maxBlockSize)
		if err != nil {
			return err
		}
		tr.topics[topic] = &registry
	}
	return nil
}

func (tr *TopicRegister) CreateTopic(topic string) (bool, error) {
	if tr.doesTopicExist(topic) {
		return false, nil
	}
	err := os.Mkdir(tr.topicsRootPath+separator+topic, 0777) //Todo: more restrictive
	if err != nil {
		return false, err
	}
	registry, err := NewBlockRegistry(tr.topicsRootPath, topic, tr.maxBlockSize)
	if err != nil {
		return false, err
	}

	tr.topics[topic] = &registry
	return true, nil
}

func (tr *TopicRegister) DropTopic(topic string) (bool, error) {

	if !tr.doesTopicExist(topic) {
		return false, nil
	}
	oldLocation := tr.topicsRootPath + separator + topic
	newLocation := tr.topicsRootPath + separator + "." + topic
	err := os.Rename(oldLocation, newLocation)
	if err != nil {
		return false, err
	}
	delete(tr.topics, topic)
	return true, nil
}

func (tr *TopicRegister) ListTopics() ([]string, error) {
	topics := make([]string, 0, len(tr.topics))
	for k := range tr.topics {
		topics = append(topics, k)
	}
	return topics, nil
}

func (tr *TopicRegister) doesTopicExist(topic string) bool {
	_, exists := tr.topics[topic]
	return exists
}

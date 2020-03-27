package ext4

import (
	"os"
)

type TopicRegister struct {
	topicsRootPath string
	topics         []string
}

func newTopics(rootPath string) (TopicRegister, error) {
	var topics []string
	register := TopicRegister{
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
	tr.topics = directories
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
	tr.topics = append(tr.topics, topic)
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
	var deletedFromTopics []string
	for _, v := range tr.topics {
		if v != topic {
			deletedFromTopics = append(deletedFromTopics, v)
		}
	}
	tr.topics = deletedFromTopics
	return true, nil
}

func (tr *TopicRegister) ListTopics() {

}

func (tr *TopicRegister) doesTopicExist(topic string) bool {
	exists := false
	for _, v := range tr.topics {
		if v == topic {
			exists = true
			break
		}
	}
	return exists
}

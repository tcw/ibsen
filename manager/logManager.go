package manager

import (
	"github.com/tcw/ibsen/access"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (access.Offset, error)
	Read(params access.ReadParams) error
}

var _ LogManager = LogTopicsManager{}

type LogTopicsManager struct {
	topics map[access.Topic]TopicManager
}

func (l LogTopicsManager) Write(topic access.Topic, entries access.Entries) (access.Offset, error) {
	panic("implement me")
}

func (l LogTopicsManager) Read(params access.ReadParams) error {
	panic("implement me")
}

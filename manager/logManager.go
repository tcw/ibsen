package manager

import (
	"github.com/tcw/ibsen/access"
	"sync"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (access.Offset, error)
	Read(params access.ReadParams) error
}

var _ LogManager = LogTopicsManager{}

type TopicManager struct {
	logBlocks   access.Blocks
	logOffset   access.Offset
	logMutex    sync.Mutex
	indexBlocks access.Blocks
	indexOffset access.Offset
	indexMutex  sync.Mutex
}

type LogTopicsManager struct {
	topics map[access.Topic]TopicManager
}

func (l LogTopicsManager) Write(topic access.Topic, entries access.Entries) (access.Offset, error) {
	panic("implement me")
}

func (l LogTopicsManager) Read(params access.ReadParams) error {
	panic("implement me")
}

func (t *TopicManager) Write(entries access.Entries) error {
	panic("implement me")
}

func (t *TopicManager) Read(params access.ReadParams) error {

}

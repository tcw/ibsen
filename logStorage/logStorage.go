package logStorage

import (
	"github.com/tcw/ibsen/api/grpc/golangApi"
	"sync"
)

type LogStorage interface {
	Create(topic *golangApi.Topic) (bool, error)
	Drop(topic *golangApi.Topic) (bool, error)
	Write(*golangApi.TopicMessage) (int, error)
	ReadFromBeginning(logChan chan *LogEntry, wg *sync.WaitGroup, topic *golangApi.Topic) error
	ReadFromNotIncluding(logChan chan *LogEntry, wg *sync.WaitGroup, topic *golangApi.Topic, offset *golangApi.Offset) error
	ListTopics() ([]golangApi.Topic, error)
}

type LogEntry struct {
	Offset   uint64
	ByteSize int
	Entry    *[]byte
}

func NewLogEntry(offset uint64, entry *[]byte) LogEntry {
	return LogEntry{
		Offset:   offset,
		ByteSize: len(*entry),
		Entry:    entry,
	}
}

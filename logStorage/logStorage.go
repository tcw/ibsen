package logStorage

import (
	"sync"
)

// Todo: add batch block write
type LogStorage interface {
	Create(topic string) (bool, error)
	Drop(topic string) (bool, error)
	Write(topicMessage *TopicMessage) (int, error)
	ReadFromBeginning(logChan chan *LogEntry, wg *sync.WaitGroup, topic string) error
	ReadFromNotIncluding(logChan chan *LogEntry, wg *sync.WaitGroup, topic string, offset uint64) error
	ListTopics() ([]string, error)
}

type TopicMessage struct {
	Topic   string
	Message *[]byte
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

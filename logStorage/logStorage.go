package logStorage

import (
	"sync"
)

type LogStorage interface {
	Create(topic string) (bool, error)
	Drop(topic string) (bool, error)
	Write(topicMessage *TopicMessage) (int, error)
	WriteBatch(topicBatchMessage *TopicBatchMessage) (int, error)
	ReadFromBeginning(logChan chan LogEntry, wg *sync.WaitGroup, topic string) error
	ReadBatchFromBeginning(logChan chan LogEntryBatch, wg *sync.WaitGroup, topic string, batchSize int) error
	ReadFromNotIncluding(logChan chan LogEntry, wg *sync.WaitGroup, topic string, offset uint64) error
	ReadBatchFromOffsetNotIncluding(logChan chan LogEntryBatch, wg *sync.WaitGroup, topic string, offset uint64, batchSize int) error
	ListTopics() ([]string, error)
}

type LogEntryBatch struct {
	Entries []LogEntry
}

func (e *LogEntryBatch) Size() int {
	return len(e.Entries)
}

type TopicMessage struct {
	Topic   string
	Message []byte
}

type TopicBatchMessage struct {
	Topic   string
	Message *[][]byte
}

type LogEntry struct {
	Offset   uint64
	ByteSize int
	Entry    []byte
}

type LogBatchEntry struct {
	Entries *[][]byte
}

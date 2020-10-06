package logStorage

import (
	"sync"
)

type LogStorage interface {
	Create(topic string) (bool, error)
	Drop(topic string) (bool, error)
	Status() ([]string, error)
	WriteBatch(topicBatchMessage *TopicBatchMessage) (int, error)
	ReadBatchFromBeginning(logChan chan LogEntryBatch, wg *sync.WaitGroup, topic string, batchSize int) error
	ReadBatchFromOffsetNotIncluding(logChan chan LogEntryBatch, wg *sync.WaitGroup, topic string, offset uint64, batchSize int) error
	Close()
}

type LogEntryBatch struct {
	Entries []LogEntry
}

func (e *LogEntryBatch) Offset() int64 {
	if e.Size() > 0 {
		entry := e.Entries[len(e.Entries)-1]
		return int64(entry.Offset)
	} else {
		return -1
	}
}

func (e *LogEntryBatch) Size() int {
	return len(e.Entries)
}

func (e *LogEntryBatch) ToArray() [][]byte {
	entries := e.Entries
	bytes := make([][]byte, e.Size())
	for i, entry := range entries {
		bytes[i] = append(bytes[i], entry.Entry[i])
	}
	return bytes
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
	Crc      uint32
	ByteSize int
	Entry    []byte
}

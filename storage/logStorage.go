package storage

import (
	"errors"
	"github.com/tcw/ibsen/commons"
	"sync"
)

type LogStorage interface {
	Create(topic string) (bool, error)
	Drop(topic string) (bool, error)
	Status() []*TopicStatusMessage
	WriteBatch(topicBatchMessage *TopicBatchMessage) (int, error)
	ReadBatch(readBatchParam ReadBatchParam) error
	ReadStreamingBatch(readBatchParam ReadBatchParam) error
	Close()
}

type ReadBatchParam struct {
	LogChan      chan *LogEntryBatch
	Wg           *sync.WaitGroup
	Topic        string
	BatchSize    int
	Offset       uint64
	IbsenIndexer commons.IndexedOffset
}

type LogEntryBatch struct {
	Entries []LogEntry
}

func (e *LogEntryBatch) FindFirst() (LogEntry, error) {
	if e.Size() > 0 {
		return e.Entries[0], nil
	} else {
		return LogEntry{}, errors.New("NOT_FOUND")
	}
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
	bytes := make([][]byte, 0)
	for _, entry := range entries {
		bytes = append(bytes, entry.Entry)
	}
	return bytes
}

type TopicBatchMessage struct {
	Topic   string
	Message [][]byte
}

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type TopicStatusMessage struct {
	Topic        string
	Blocks       int
	Offset       int64
	MaxBlockSize int64
	Path         string
}

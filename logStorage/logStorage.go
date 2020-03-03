package logStorage

import "sync"

type LogStorage interface {
	Create(topic Topic) (bool, error)
	Drop(topic Topic) (bool, error)
	Write(topic Topic, entry Entry) (int, error)
	ReadFromBeginning(logChan chan *LogEntry, wg *sync.WaitGroup, topic Topic) error
	ReadFromNotIncluding(logChan chan *LogEntry, wg *sync.WaitGroup, topic Topic, offset Offset) error
	ListTopics() ([]Topic, error)
}

type LogEntry struct {
	Offset   Offset
	ByteSize int
	Entry    Entry
}

type Offset uint64

type Entry []byte

type Topic string

func NewLogEntry(offset Offset, entry Entry) LogEntry {
	return LogEntry{
		Offset:   offset,
		ByteSize: len(entry),
		Entry:    entry,
	}
}

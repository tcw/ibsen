package manager

import (
	"github.com/tcw/ibsen/access"
	"sync"
)

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type ReadParams struct {
	Topic           access.Topic
	Offset          access.Offset
	NumberOfEntries access.Entries
	TTL             access.TTL
	LogChan         chan *[]LogEntry
	Wg              *sync.WaitGroup
}

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (access.Offset, error)
	Read(params ReadParams) error
}

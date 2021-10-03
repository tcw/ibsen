package manager

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"sync"
)

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type ReadParams struct {
	Topic           commons.Topic
	Offset          commons.Offset
	NumberOfEntries commons.Entries
	TTL             commons.TTL
	LogChan         chan *[]LogEntry
	Wg              *sync.WaitGroup
}

type LogManager interface {
	Write(topic commons.Topic, entries commons.Entries) (commons.Offset, error)
	Read(params ReadParams) error
}

package common

import (
	"errors"
	"github.com/spf13/afero"
	"sync"
)

type Offset uint64
type LogBlock uint64
type IndexBlock uint64
type BlockIndex uint32

var NoBlocksFound = errors.New("no blocks found")

var NoEntriesFound = errors.New("no entries found")

type LogBlockPosition struct {
	Block      LogBlock
	ByteOffset int64
}

type EntriesPtr *[][]byte

type ReadLogParams struct {
	LogChan   chan *[]LogEntry
	Wg        *sync.WaitGroup
	From      Offset
	BatchSize uint32
}

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type TopicParams struct {
	Afs          *afero.Afero
	RootPath     string
	TopicName    string
	MaxBlockSize int
}

type OffsetFilePtr struct {
	Offset     Offset
	ByteOffset int64
}

type OffsetPosition struct {
	logBlock       LogBlock
	byteOffset     int64
	entriesScanned int
	indexEntryUsed OffsetFilePtr
}

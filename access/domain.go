package access

import (
	"hash/crc32"
	"os"
	"sync"
)

const Sep = string(os.PathSeparator)

type Offset uint64
type Topic string
type Entries *[][]byte
type BlockSizeInBytes uint64
type FileName string
type StrictlyMonotonicOrderedVarIntIndex []byte

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type ReadParams struct {
	Topic            Topic
	Offset           Offset
	StopOnCompletion bool
	BatchSize        uint32
	LogChan          chan *[]LogEntry
	Wg               *sync.WaitGroup
}

var crc32q = crc32.MakeTable(crc32.Castagnoli)

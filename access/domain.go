package access

import (
	"sync"
)

type BlockSizeInBytes uint64
type FileName string
type StrictlyMonotonicOrderedVarIntIndex []byte

type ReadParams struct {
	Topic            Topic
	Offset           Offset
	StopOnCompletion bool
	BatchSize        uint32
	LogChan          chan *[]LogEntry
	Wg               *sync.WaitGroup
}

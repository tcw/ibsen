package common

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"sync"
)

type Offset uint64
type LogBlock uint64
type IndexBlock uint64
type BlockIndex uint32
type TopicName string

var NoBlocksFound = errors.New("no blocks found")

var NoEntriesFound = errors.New("no entries found")

var ErrReadCancelled = errors.New("read cancelled")

var ErrInvalidTopicName = errors.New("invalid topic name")

// MaxTopicNameLength is the longest topic name in bytes. A topic is a directory, and most
// filesystems limit a name to 255 bytes.
const MaxTopicNameLength = 255

// ValidateTopicName rejects names that are unsafe as a topic directory: empty or too long
// names, a leading dot (which covers "." and ".." and would hide the topic from listing),
// path separators, and control characters.
func ValidateTopicName(name TopicName) error {
	if len(name) == 0 || len(name) > MaxTopicNameLength {
		return fmt.Errorf("%w: length must be 1 to %d bytes", ErrInvalidTopicName, MaxTopicNameLength)
	}
	if name[0] == '.' {
		return fmt.Errorf("%w: %q starts with a dot", ErrInvalidTopicName, name)
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; c == '/' || c == '\\' || c < 0x20 || c == 0x7f {
			return fmt.Errorf("%w: %q contains %q", ErrInvalidTopicName, name, c)
		}
	}
	return nil
}

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
	// Cancel stops the read with ErrReadCancelled when closed; nil never cancels.
	Cancel <-chan struct{}
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

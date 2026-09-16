package common

import (
	"errors"
	"fmt"
	"io"
)

// BlockStore is the storage port. A log is a set of topics, each holding blocks of bytes
// named by the offset of their first entry. The core speaks only these verbs, so a backend
// can be a filesystem, a byte slice in RAM or a raw flash region without the core knowing.
//
// The block verbs are List, Append, Open and Remove. Truncate exists because crash recovery
// has to cut a torn tail off the head block, and the two topic verbs exist because a log
// server has to enumerate and create topics. It is deliberately not a filesystem
// abstraction: there are no directories, handles, seeks or permissions.
//
// Implementations must be safe for concurrent use.
type BlockStore interface {
	// Topics lists the topics the store holds, in no particular order.
	Topics() ([]TopicName, error)

	// CreateTopic makes a topic that can hold blocks and reports whether this call created
	// it. Creating a topic that already exists is not an error.
	CreateTopic(topic TopicName) (created bool, err error)

	// List returns the blocks of one kind held by a topic, ordered by block, each with the
	// number of bytes it currently holds. An unknown topic has no blocks and is not an error.
	List(topic TopicName, kind BlockKind) ([]Block, error)

	// Append adds data to the end of a block, creating the block, and the topic, if needed.
	// It is all or nothing: on error the block is left as it was before the call, and the
	// error wraps ErrDirtyBlock if even that could not be guaranteed. It returns the block
	// with its new size. Appending no bytes still creates the block.
	Append(ref BlockRef, data []byte) (Block, error)

	// Open returns a reader over a block, starting at byteOffset. Reading past the end of
	// the block gives io.EOF, as does a byteOffset at or past the end. An unknown block
	// gives ErrBlockNotFound. The caller closes the reader.
	Open(ref BlockRef, byteOffset int64) (io.ReadCloser, error)

	// Truncate cuts a block down to size bytes, which must not be larger than the block.
	Truncate(ref BlockRef, size int64) error

	// Remove deletes a block. An unknown block gives ErrBlockNotFound.
	Remove(ref BlockRef) error
}

// Syncable is the optional durability capability of a BlockStore, probed by type assertion
// rather than required of every backend: an in-memory store has nothing to sync, and a
// microcontroller writing straight to flash has already persisted the bytes.
type Syncable interface {
	// Sync returns once every byte appended to the block is on durable media.
	Sync(ref BlockRef) error
}

// Sync flushes a block if its store is Syncable, and reports whether the store was. A store
// that cannot sync is not an error: it has no volatile buffer to lose.
func Sync(store BlockStore, ref BlockRef) (bool, error) {
	syncable, ok := store.(Syncable)
	if !ok {
		return false, nil
	}
	return true, syncable.Sync(ref)
}

// BlockKind separates the two kinds of block a topic holds, so one store can keep both
// without the core inventing file names.
type BlockKind uint8

const (
	// Log holds entries as written by CreateByteEntry.
	Log BlockKind = iota
	// Index holds (offset, byteOffset) pairs into the log block with the same number.
	Index
)

func (k BlockKind) String() string {
	switch k {
	case Log:
		return "log"
	case Index:
		return "index"
	default:
		return fmt.Sprintf("BlockKind(%d)", uint8(k))
	}
}

// BlockRef names one block of one topic.
type BlockRef struct {
	Topic TopicName
	Kind  BlockKind
	Block uint64
}

func (r BlockRef) String() string {
	return fmt.Sprintf("%s/%020d.%s", r.Topic, r.Block, r.Kind)
}

// LogRef refers to the log block starting at offset.
func LogRef(topic TopicName, block LogBlock) BlockRef {
	return BlockRef{Topic: topic, Kind: Log, Block: uint64(block)}
}

// IndexRef refers to the index of the log block starting at offset.
func IndexRef(topic TopicName, block IndexBlock) BlockRef {
	return BlockRef{Topic: topic, Kind: Index, Block: uint64(block)}
}

// Block is a block and the number of bytes it holds.
type Block struct {
	Block uint64
	Size  int64
}

var (
	// ErrBlockNotFound is returned for a block the store does not hold.
	ErrBlockNotFound = errors.New("block not found")

	// ErrInvalidSize is returned by Truncate for a size larger than the block.
	ErrInvalidSize = errors.New("invalid block size")

	// ErrDirtyBlock is wrapped into a failed Append that could not be rolled back. The block
	// may end in a partial write, so it must be recovered before it is appended to again.
	ErrDirtyBlock = errors.New("block may hold a partial append")
)

// DirtyBlock marks cause as a failure that left a block possibly holding a partial append.
// The result matches both ErrDirtyBlock and cause.
func DirtyBlock(cause error) error {
	return &dirtyBlock{cause: cause}
}

type dirtyBlock struct {
	cause error
}

func (d *dirtyBlock) Error() string {
	return ErrDirtyBlock.Error() + ": " + d.cause.Error()
}

func (d *dirtyBlock) Unwrap() error {
	return d.cause
}

func (d *dirtyBlock) Is(target error) bool {
	return target == ErrDirtyBlock
}

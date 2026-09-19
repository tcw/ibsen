// Package embedded is the composition root for a build with no server around it: the log as
// a library inside another program, or on a microcontroller.
//
// It is the counterpart to the wiring package, which assembles the full server. The
// difference is not a build tag. This is a separate package that imports nothing but the
// core, so a program that imports it links no gRPC, no cobra, no OTEL, no zerolog and no
// compressor. scripts/check-architecture.sh holds it to that with the same rule it
// holds the core to, which is worth more than a tag: a tag has to be trusted, a dependency
// graph can be read.
//
// What a build pays for is what it brings. The store is injected rather than chosen here, so
// a program bringing memstore, flashstore or filestore stays inside the standard library, and
// one wiring the zstd codec pays for zstd. Nothing in this package decides that for it.
//
// Nothing here is woken on a timer: indexing is driven by the write that dirtied the index,
// and flushing by the writer that needs its entries durable.
package embedded

import (
	"errors"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
)

// DefaultMaxBlockSize is the block size an embedded build gets when it does not choose one.
// It is far smaller than the server's default gigabyte, because the sort of device this
// package exists for does not have a gigabyte. A program with a disk under it should say so.
const DefaultMaxBlockSize = 1 << 20

// ErrNoStore is returned by Open without a BlockStore. There is no default: where the bytes
// go is the one decision an embedded build has to make, and guessing at it would be the
// package quietly choosing a dependency for the program that imports it.
var ErrNoStore = errors.New("embedded: a BlockStore is required")

// Params is everything an embedded log takes. Every field but Store has a working default,
// and the defaults are the core's own.
type Params struct {
	// Store is where blocks live: memstore, flashstore, filestore, or a caller's own.
	Store driven.BlockStore
	// ReadOnly refuses writes, for a build that only consumes a log another wrote.
	ReadOnly bool
	// MaxBlockSize is the size at which a topic rolls over to a new block; zero means
	// DefaultMaxBlockSize.
	MaxBlockSize int
	// IndexSparsity is the entries between two index pairs; zero means the core default.
	IndexSparsity uint32
	// FlushEntries and FlushInterval are the durability policy. Zero entries means every
	// write is durable before it is acknowledged. A store that cannot sync ignores both,
	// which is why memstore and flashstore pay nothing for durability.
	FlushEntries  uint32
	FlushInterval time.Duration
	// MaxFrameEntries and MaxFrameBytes bound one frame; zero means the core defaults.
	MaxFrameEntries uint32
	MaxFrameBytes   int
	// Codec compresses the frames this build writes, and Codecs resolves the codec byte of
	// frames already written. Leaving both out writes and reads uncompressed frames and
	// links no compressor.
	Codec  driven.Codec
	Codecs driven.Codecs
	// Logger is where the log says what it did; nil says nothing and links nothing.
	Logger driven.Logger
}

// Log is an embedded log. It is the same driving port a gRPC server speaks, so code written
// against one works against the other.
type Log struct {
	manager manager.LogTopicsManager
}

var _ driver.LogManager = (*Log)(nil)

// Open builds the log. The caller closes it, which waits for writes and the indexing they
// started.
func Open(params Params) (*Log, error) {
	if params.Store == nil {
		return nil, ErrNoStore
	}
	if params.MaxBlockSize == 0 {
		params.MaxBlockSize = DefaultMaxBlockSize
	}
	built, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		ReadOnly:        params.ReadOnly,
		Store:           params.Store,
		MaxBlockSize:    params.MaxBlockSize,
		IndexSparsity:   params.IndexSparsity,
		MaxFrameEntries: params.MaxFrameEntries,
		MaxFrameBytes:   params.MaxFrameBytes,
		Codec:           params.Codec,
		Codecs:          params.Codecs,
		FlushEntries:    params.FlushEntries,
		FlushInterval:   params.FlushInterval,
		Logger:          params.Logger,
	})
	if err != nil {
		return nil, err
	}
	return &Log{manager: built}, nil
}

// List names the topics the log holds.
func (l *Log) List() []domain.TopicName {
	return l.manager.List()
}

// Write appends entries to a topic, creating it if it is new, and returns once they are on
// durable media.
func (l *Log) Write(topic domain.TopicName, entries domain.EntriesPtr) error {
	return l.manager.Write(topic, entries)
}

// Read sends entries from an offset onwards to the caller's channel.
func (l *Log) Read(params driver.ReadParams) error {
	return l.manager.Read(params)
}

// Close refuses further writes and waits for those in flight and for the indexing they
// started. Loaded topics stay readable.
func (l *Log) Close() {
	l.manager.Close()
}

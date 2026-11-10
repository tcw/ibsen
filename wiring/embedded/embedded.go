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
// Nothing here is woken on a timer unless the program asks for one: indexing is driven by
// the write that dirtied the index, and in strict mode flushing by the writer that needs its
// entries durable. Lenient mode needs something to call Flush; FlushInterval asks this
// package to start a ticker that does, and leaving it zero leaves Flush to the program.
package embedded

import (
	"errors"
	"sync"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
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
	// FlushMode says when a write is acknowledged: topic.FlushStrict (the zero value) once
	// it is durable, topic.FlushLenient once it is appended, with Flush making it durable
	// and readable later.
	FlushMode topic.FlushMode
	// FlushEntries and FlushInterval are the durability policy. In strict mode, zero entries
	// means every write is durable before it is acknowledged, and the interval is how long a
	// batch may wait for company. In lenient mode FlushInterval is the tick: Open starts a
	// goroutine calling Flush that often, stopped by Close, and zero starts nothing and
	// leaves Flush to the program. A store that cannot sync ignores all of it, which is why
	// memstore and flashstore pay nothing for durability.
	FlushEntries  uint32
	FlushInterval time.Duration
	// OnFlushError is told when a flush the ticker made failed; nil ignores it. The failure
	// also stops the topic, and its next Write, Flush and Close return it.
	OnFlushError func(error)
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
	// ProducerWindowBlocks is how many of a topic's newest blocks a producer is remembered
	// across for safe retries; zero means the core default.
	ProducerWindowBlocks int
	// Dictionaries trains a compression dictionary per topic, when Codec can use one: only a
	// codec adapter that implements driven.DictionaryCodec can, so a build without one links
	// nothing for it. DictTrainBytes is when; zero means the core default.
	Dictionaries   bool
	DictTrainBytes int64
	// CompressMinBytes and AdaptiveCompression decide which frames are offered to the codec
	// at all; the zero values try every frame. See topic.Params.
	CompressMinBytes    int
	AdaptiveCompression bool
}

// Log is an embedded log. It is the same driving port a gRPC server speaks, so code written
// against one works against the other.
type Log struct {
	manager    manager.LogTopicsManager
	stopTicker func()
}

var _ driver.LogManager = (*Log)(nil)
var _ driver.Flusher = (*Log)(nil)

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
		ReadOnly:             params.ReadOnly,
		Store:                params.Store,
		MaxBlockSize:         params.MaxBlockSize,
		IndexSparsity:        params.IndexSparsity,
		MaxFrameEntries:      params.MaxFrameEntries,
		MaxFrameBytes:        params.MaxFrameBytes,
		Codec:                params.Codec,
		Codecs:               params.Codecs,
		FlushEntries:         params.FlushEntries,
		FlushInterval:        params.FlushInterval,
		FlushMode:            params.FlushMode,
		ProducerWindowBlocks: params.ProducerWindowBlocks,
		Dictionaries:         params.Dictionaries,
		DictTrainBytes:       params.DictTrainBytes,
		CompressMinBytes:     params.CompressMinBytes,
		AdaptiveCompression:  params.AdaptiveCompression,
		Logger:               params.Logger,
	})
	if err != nil {
		return nil, err
	}
	opened := &Log{manager: built, stopTicker: func() {}}
	if params.FlushMode == topic.FlushLenient && params.FlushInterval > 0 && !params.ReadOnly {
		opened.stopTicker = StartFlushTicker(opened, params.FlushInterval, params.OnFlushError)
	}
	return opened, nil
}

// StartFlushTicker calls f.Flush every interval until the returned stop is called, which
// waits for a flush under way to finish. It is the timer the core does not start, for a
// composition root to own: this one in lenient mode, and the server's. onError, if not nil,
// is told about every flush that failed.
func StartFlushTicker(f driver.Flusher, interval time.Duration, onError func(error)) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	var stopped sync.WaitGroup
	stopped.Add(1)
	go func() {
		defer stopped.Done()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := f.Flush(); err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			ticker.Stop()
			close(done)
			stopped.Wait()
		})
	}
}

// List names the topics the log holds.
func (l *Log) List() []domain.TopicName {
	return l.manager.List()
}

// Write appends entries to a topic, creating it if it is new. In strict mode it returns once
// they are on durable media; in lenient mode once they are appended, and Flush makes them
// durable.
func (l *Log) Write(params driver.WriteParams) (domain.WriteResult, error) {
	return l.manager.Write(params)
}

// Read sends entries from an offset onwards to the caller's channel.
func (l *Log) Read(params driver.ReadParams) error {
	return l.manager.Read(params)
}

// Retain deletes the oldest whole blocks of every topic while policy says to, never a
// topic's head block. now is the program's clock. Nothing calls it for an embedded log; a
// program that wants retention calls it when it likes.
func (l *Log) Retain(policy func(domain.TopicName) topic.RetentionPolicy, now time.Time) error {
	return l.manager.Retain(policy, now)
}

// Flush makes everything written so far durable, and readable, and returns the first error.
func (l *Log) Flush() error {
	return l.manager.Flush()
}

// Close stops the flush ticker if there is one, refuses further writes, waits for those in
// flight and for the indexing they started, and flushes, so a clean exit loses nothing. It
// returns the first flush error. Loaded topics stay readable.
func (l *Log) Close() error {
	l.stopTicker()
	return l.manager.Close()
}

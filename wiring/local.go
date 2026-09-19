package wiring

import (
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	zstdcodec "github.com/tcw/ibsen/adapter/driven/compression/zstd"
	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/adapter/driven/logging/zerologger"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/wiring/embedded"
)

// LocalLog is a data directory opened directly, with no server between a program and the
// bytes. It is the composition root behind `ibsen append` and `ibsen cat`: the same core,
// the same filestore and the same single-writer lease the server uses, assembled for one
// process that opens the log, does its work and closes it again.
//
// It embeds an *embedded.Log, so it satisfies driver.LogManager and a driving adapter cannot
// tell it apart from the log a server holds.
type LocalLog struct {
	*embedded.Log
	lock driven.SingleIbsenWriterLock
	zstd *zstdcodec.Codec
	root string
}

// LocalParams is what opening a data directory takes. Everything but RootPath has a working
// default, and the defaults are the ones the server uses.
type LocalParams struct {
	// RootPath is the data directory, which must exist.
	RootPath string
	// ReadOnly builds a store that refuses every call that would change a file, and takes no
	// lock. It is what makes a reader safe to point at a directory another instance owns:
	// loading a topic recovers its head block and could otherwise truncate a torn tail
	// underneath the writer. A read-only open of a damaged topic fails rather than repairing
	// it, which is the right answer for a process that does not own the log.
	ReadOnly bool
	// MaxBlockSize is the size at which a topic rolls over to a new block; zero means the
	// embedded default.
	MaxBlockSize int
	// IndexSparsity is the entries between two index pairs; zero means the core default.
	IndexSparsity uint32
	// MaxFrameEntries and MaxFrameBytes bound one frame; zero means the core defaults.
	MaxFrameEntries uint32
	MaxFrameBytes   int
	// FlushEntries and FlushInterval are the durability policy; zero entries flushes every
	// write before acknowledging it.
	FlushEntries  uint32
	FlushInterval time.Duration
	// Compression names the codec frames are written with, and CompressionLevel tunes it.
	// Every codec this binary links can be read whatever is written with.
	Compression      string
	CompressionLevel string
	// Lock is optional: a writable open builds a file lease over RootPath when none is
	// given, the same lease at the same path the server takes.
	Lock driven.SingleIbsenWriterLock
	// Logger is optional; nil logs through zerolog, like the server.
	Logger driven.Logger
}

// OpenLocal opens a data directory for one process.
//
// A writable open takes the single-writer lease, so it is refused with ErrWriteLockUnavailable
// while a server, or another append, holds the directory. That is the lock doing its job
// rather than a limitation: two writers on one log is the outcome it exists to prevent. A
// read-only open takes no lease at all and can be pointed at a live directory.
func OpenLocal(params LocalParams) (*LocalLog, error) {
	exists, err := rootExists(params.RootPath)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	if !exists {
		return nil, errore.NewF("path [%s] does not exist, will not open unless existing path is specified",
			params.RootPath)
	}

	codec, codecs, zstd, err := buildCodecs(params.Compression, params.CompressionLevel, nil, nil)
	if err != nil {
		return nil, err
	}

	lock := params.Lock
	if !params.ReadOnly {
		if lock == nil {
			lock = locking.NewFileLock(
				filepath.Join(params.RootPath, writeLockFileName), writeLockLease, writeLockReclaim,
				func(reason error) {
					// the same reasoning as the server's: whatever this process still holds
					// is already lost, and a clean shutdown would flush and write on the way
					// out, so the only safe move is to stop touching the data directory
					log.Fatal().Err(reason).Msgf(
						"lost the single writer lock on [%s], stopping before another instance writes beside us",
						params.RootPath)
				})
		}
		if !lock.AcquireLock() {
			zstd.Close()
			return nil, errore.WrapWithContextF(ErrWriteLockUnavailable,
				"unable to acquire the single writer lock on path [%s]", params.RootPath)
		}
	}

	logger := params.Logger
	if logger == nil {
		logger = zerologger.New(log.Logger)
	}
	opened, err := embedded.Open(embedded.Params{
		Store:           localStore(params),
		ReadOnly:        params.ReadOnly,
		MaxBlockSize:    params.MaxBlockSize,
		IndexSparsity:   params.IndexSparsity,
		MaxFrameEntries: params.MaxFrameEntries,
		MaxFrameBytes:   params.MaxFrameBytes,
		FlushEntries:    params.FlushEntries,
		FlushInterval:   params.FlushInterval,
		Codec:           codec,
		Codecs:          codecs,
		Logger:          logger,
	})
	if err != nil {
		zstd.Close()
		if !params.ReadOnly {
			lock.ReleaseLock()
		}
		return nil, err
	}
	return &LocalLog{Log: opened, lock: lock, zstd: zstd, root: params.RootPath}, nil
}

// localStore is where the bytes go: the filesystem adapter, read-only or not.
func localStore(params LocalParams) driven.BlockStore {
	if params.ReadOnly {
		return filestore.New(filestore.ReadOnly{FS: filestore.OS{}}, params.RootPath)
	}
	return filestore.NewOS(params.RootPath)
}

// Close finishes what is under way and releases the directory, in the order the server's
// shutdown uses: the log first, because closing it waits for writes and the indexing they
// started; then the codec's buffers, once nothing reads or writes a frame; then the lease,
// once nothing will write again.
func (l *LocalLog) Close() {
	l.Log.Close()
	if l.zstd != nil {
		l.zstd.Close()
	}
	if l.lock != nil {
		l.lock.ReleaseLock()
	}
}

// Package logfmt reads and recovers log blocks through the driven.BlockStore port. It knows
// how a block is framed and how an entry is encoded, and nothing about where the bytes live.
package logfmt

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

type BlockSizeInBytes uint64
type FileName string
type StrictlyMonotonicVarIntIndex []byte

// maxBatchBytes caps the payload bytes collected into one read batch.
const maxBatchBytes = 10 * 1024 * 1024

// maxBatchPrealloc caps the entries preallocated for a batch, since the batch size comes from clients.
const maxBatchPrealloc = 1024

var NoByteOffsetFound = errors.New("no byte offset found")

// FindFrameByteOffset scans a log block for the frame holding offset and returns where that
// frame starts. startAtByteOffset must be a frame boundary, which is what the index stores.
//
// Nothing is decoded and no payload is read: a header says which offsets its frame holds and
// how many bytes it takes, so the scan steps over whole frames. That is the difference
// between this and the entry-by-entry scan it replaced.
func FindFrameByteOffset(store driven.BlockStore, ref driven.BlockRef, startAtByteOffset int64, offset domain.Offset) (int64, int, error) {
	block, err := store.Open(ref, startAtByteOffset)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	defer block.Close()
	reader := bufio.NewReader(block)

	byteOffset := startAtByteOffset
	scanCount := 0
	for {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err == io.EOF {
			return 0, scanCount, NoByteOffsetFound
		}
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		if header.Contains(offset) {
			return byteOffset, scanCount, nil
		}
		if header.FirstOffset > offset {
			// the scan started past the offset asked for, so the block does not hold it
			return 0, scanCount, NoByteOffsetFound
		}
		if err = domain.SkipFramePayload(reader, header); err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		byteOffset = byteOffset + header.Size()
		scanCount = scanCount + 1
	}
}

// RecoverBlock verifies every frame of a log block from the start and truncates a torn tail
// left by an interrupted write. blockSize is the size the store reports for the block. It
// returns the offset following the last valid frame, the valid size of the block in bytes,
// and the number of bytes truncated.
//
// It decodes nothing: a frame is checked against the two checksums in its header, so a build
// that does not carry the codec a block was written with can still cut its torn tail. A
// valid frame with an unexpected offset is corruption rather than a torn write and is
// returned as an error without truncating, as is a block this build cannot read at all.
func RecoverBlock(store driven.BlockStore, ref driven.BlockRef, firstOffset domain.Offset, blockSize int64) (domain.Offset, int64, int64, error) {
	nextOffset, validSize, err := scanValidFrames(store, ref, firstOffset, blockSize)
	if err != nil {
		return 0, 0, 0, err
	}
	truncated := blockSize - validSize
	if truncated == 0 {
		return nextOffset, validSize, 0, nil
	}
	if err = store.Truncate(ref, validSize); err != nil {
		return 0, 0, 0, errore.Wrap(err)
	}
	return nextOffset, validSize, truncated, nil
}

func scanValidFrames(store driven.BlockStore, ref driven.BlockRef, firstOffset domain.Offset, blockSize int64) (domain.Offset, int64, error) {
	block, err := store.Open(ref, 0)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	defer block.Close()
	reader := bufio.NewReader(block)
	nextOffset := firstOffset
	var validSize int64
	for {
		// a payload cannot be larger than what is left of the block
		var maxStored uint64
		if remaining := blockSize - validSize - domain.FrameHeaderSize; remaining > 0 {
			maxStored = uint64(remaining)
		}
		header, err := domain.ReadFrameHeader(reader, maxStored)
		if err == io.EOF {
			return nextOffset, validSize, nil
		}
		if validSize == 0 && errors.Is(err, domain.ErrNotAFrame) {
			// nothing in this block was ever a frame, so there is no torn tail to cut and
			// nothing here to guess at: say so rather than truncate bytes we do not understand
			return 0, 0, errore.WrapWithContextF(domain.ErrUnsupportedLogFormat,
				"log block %s does not start with a frame header: written before framing, or damaged", ref)
		}
		if errors.Is(err, domain.ErrUnsupportedLogFormat) {
			return 0, 0, errore.Wrap(err)
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, domain.ErrCorruptFrame) {
			return nextOffset, validSize, nil
		}
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		err = domain.VerifyFramePayload(reader, header)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, domain.ErrCorruptFrame) {
			return nextOffset, validSize, nil
		}
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		if header.FirstOffset != nextOffset {
			return 0, 0, errore.NewF("log block %s: frame at byte %d starts at offset %d, expected %d",
				ref, validSize, header.FirstOffset, nextOffset)
		}
		nextOffset = header.EndOffset()
		validSize = validSize + header.Size()
	}
}

type ReadFileParams struct {
	// Reader holds the log block, positioned at the start of the frame holding FromOffset.
	Reader io.Reader
	// Codecs resolves the codec byte a frame carries. A nil registry reads frames written
	// without compression and reports the rest as unknown.
	Codecs    driven.Codecs
	LogChan   chan *[]domain.LogEntry
	Wg        *sync.WaitGroup
	BatchSize uint32
	// Cancel stops the read with domain.ErrReadCancelled when closed; nil never cancels.
	Cancel <-chan struct{}
	// FromOffset is the first offset to send. The frame holding it may start earlier, and
	// the entries before it are decoded and dropped. Zero sends from the first entry there
	// is, for a reader whose first offset the caller does not know.
	FromOffset domain.Offset
	EndOffset  domain.Offset
}

type ReadResult struct {
	LastLogOffset domain.Offset
	EntriesRead   uint64
}

func (r *ReadResult) Update(result ReadResult) {
	r.LastLogOffset = result.LastLogOffset
	r.EntriesRead = r.EntriesRead + result.EntriesRead
}

func (r *ReadResult) NextOffset() domain.Offset {
	return r.LastLogOffset + 1
}

func ReadFile(params ReadFileParams) (ReadResult, error) {
	if params.BatchSize == 0 {
		return ReadResult{}, errore.New("batch size must be greater than zero")
	}
	currentOffset := params.FromOffset
	var offsetFromLogg domain.Offset = 0
	if currentOffset > 0 {
		offsetFromLogg = currentOffset - 1
	}
	var entriesRead uint64 = 0
	reader := bufio.NewReader(params.Reader)
	batchCapacity := params.BatchSize
	if batchCapacity > maxBatchPrealloc {
		batchCapacity = maxBatchPrealloc
	}
	logEntries := make([]domain.LogEntry, 0, batchCapacity)
	currentBatchInBytes := 0
	// sendBatch hands the batch to the consumer, or gives up if the read is cancelled
	// so an abandoned read never blocks on a batch nobody will receive.
	sendBatch := func() error {
		select {
		case <-params.Cancel:
			return domain.ErrReadCancelled
		default:
		}
		params.Wg.Add(1)
		batch := logEntries
		select {
		case params.LogChan <- &batch:
		case <-params.Cancel:
			params.Wg.Done()
			return domain.ErrReadCancelled
		}
		logEntries = make([]domain.LogEntry, 0, batchCapacity)
		currentBatchInBytes = 0
		return nil
	}
	// complete sends whatever the last batch holds and reports how far the read got
	complete := func() (ReadResult, error) {
		if len(logEntries) > 0 {
			if err := sendBatch(); err != nil {
				return ReadResult{}, errore.Wrap(err)
			}
		}
		return ReadResult{
			LastLogOffset: offsetFromLogg,
			EntriesRead:   entriesRead,
		}, nil
	}
	for {
		if currentOffset == params.EndOffset {
			return complete()
		}
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err == io.EOF {
			return complete()
		}
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		// a frame that ends before the read begins is stepped over, not decoded
		if header.EndOffset() <= params.FromOffset {
			if err = domain.SkipFramePayload(reader, header); err != nil {
				return ReadResult{}, errore.Wrap(err)
			}
			continue
		}
		payload, err := domain.ReadFramePayload(reader, header)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		entries, err := DecodeFrame(params.Codecs, header, payload)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		frame := bytes.NewReader(entries)
		for i := uint32(0); i < header.EntryCount; i++ {
			entry, _, err := domain.ReadEntry(frame, domain.MaxEntrySize)
			if err != nil {
				return ReadResult{}, errore.Wrap(err)
			}
			offset := domain.Offset(entry.Offset)
			if expected := header.FirstOffset + domain.Offset(i); offset != expected {
				return ReadResult{}, errore.NewF("frame at offset %d holds offset %d where %d was expected",
					header.FirstOffset, offset, expected)
			}
			if offset < params.FromOffset {
				// the frame began before the read did
				continue
			}
			if currentOffset == 0 {
				currentOffset = offset
			}
			if currentOffset != offset {
				return ReadResult{}, errore.NewF("read order assertion failed, expected [%d] actual [%d]", currentOffset, offset)
			}
			if currentOffset == params.EndOffset {
				return complete()
			}
			if len(logEntries) == int(params.BatchSize) || currentBatchInBytes > maxBatchBytes {
				if err := sendBatch(); err != nil {
					return ReadResult{}, errore.Wrap(err)
				}
			}
			logEntries = append(logEntries, entry)
			currentBatchInBytes = currentBatchInBytes + entry.ByteSize
			offsetFromLogg = offset
			entriesRead = entriesRead + 1
			currentOffset = currentOffset + 1
		}
	}
}

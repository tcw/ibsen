// Package log reads and recovers log blocks through the common.BlockStore port. It knows
// the entry format and nothing about where the bytes live.
package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
)

type BlockSizeInBytes uint64
type FileName string
type StrictlyMonotonicVarIntIndex []byte

// maxBatchBytes caps the payload bytes collected into one read batch.
const maxBatchBytes = 10 * 1024 * 1024

// maxBatchPrealloc caps the entries preallocated for a batch, since the batch size comes from clients.
const maxBatchPrealloc = 1024

// offsetFieldSize is the trailing offset of an entry, which is also what a look back reads.
const offsetFieldSize = 8

var NoByteOffsetFound = errors.New("no byte offset found")

// FindByteOffsetFromAndIncludingOffset scans a log block for the entry with the given
// offset and returns where it starts. startAtByteOffset must be an entry boundary; the
// entry ending there is used to skip the scan when it is already the one before offset.
func FindByteOffsetFromAndIncludingOffset(store common.BlockStore, ref common.BlockRef, startAtByteOffset int64, offset common.Offset) (int64, int, error) {
	scanCount := 0
	if offset == 0 {
		return 0, 0, nil
	}
	openAt := startAtByteOffset
	if startAtByteOffset > 0 {
		openAt = startAtByteOffset - offsetFieldSize
	}
	block, err := store.Open(ref, openAt)
	if err != nil {
		return 0, scanCount, errore.Wrap(err)
	}
	defer block.Close()
	reader := bufio.NewReader(block)

	if startAtByteOffset > 0 {
		lastOffset, err := offsetLookBack(reader)
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		if lastOffset+1 == offset {
			return startAtByteOffset, 0, nil
		}
	}

	byteOffset := startAtByteOffset
	for {
		entry, n, err := common.ReadEntry(reader, common.MaxEntrySize)
		if err == io.EOF {
			return 0, scanCount, NoByteOffsetFound
		}
		if err != nil {
			return 0, scanCount, errore.Wrap(err)
		}
		byteOffset = byteOffset + int64(n)
		scanCount = scanCount + 1
		if common.Offset(entry.Offset+1) == offset {
			return byteOffset, scanCount, nil
		}
	}
}

// offsetLookBack reads the offset field an entry ends with, so a reader opened right after
// an entry can tell which offset comes next.
func offsetLookBack(r io.Reader) (common.Offset, error) {
	bytes := make([]byte, offsetFieldSize)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return 0, errore.Wrap(err)
	}
	return common.Offset(binary.LittleEndian.Uint64(bytes)), nil
}

// ReadEntryAt reads and verifies the entry starting at byteOffset in a log block.
func ReadEntryAt(store common.BlockStore, ref common.BlockRef, byteOffset int64) (common.LogEntry, int, error) {
	block, err := store.Open(ref, byteOffset)
	if err != nil {
		return common.LogEntry{}, 0, errore.Wrap(err)
	}
	defer block.Close()
	entry, n, err := common.ReadEntry(block, common.MaxEntrySize)
	if err != nil {
		return common.LogEntry{}, 0, errore.Wrap(err)
	}
	return entry, n, nil
}

// RecoverBlock verifies every entry of a log block from the start and truncates a torn
// tail left by an interrupted write. blockSize is the size the store reports for the block.
// It returns the offset following the last valid entry, the valid size of the block in
// bytes, and the number of bytes truncated. A valid entry with an unexpected offset is
// corruption rather than a torn write and is returned as an error without truncating.
func RecoverBlock(store common.BlockStore, ref common.BlockRef, firstOffset common.Offset, blockSize int64) (common.Offset, int64, int64, error) {
	nextOffset, validSize, err := scanValidEntries(store, ref, firstOffset, blockSize)
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

func scanValidEntries(store common.BlockStore, ref common.BlockRef, firstOffset common.Offset, blockSize int64) (common.Offset, int64, error) {
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
		var maxSize uint64
		if remaining := blockSize - validSize; remaining >= common.EntryOverhead {
			maxSize = uint64(remaining - common.EntryOverhead)
		}
		entry, n, err := common.ReadEntry(reader, maxSize)
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, common.ErrCorruptEntry) {
			return nextOffset, validSize, nil
		}
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		if common.Offset(entry.Offset) != nextOffset {
			return 0, 0, errore.NewF("log block %s: entry at byte %d has offset %d, expected %d",
				ref, validSize, entry.Offset, nextOffset)
		}
		nextOffset = nextOffset + 1
		validSize = validSize + int64(n)
	}
}

type ReadFileParams struct {
	// Reader holds the log block, positioned at the entry FromOffset names.
	Reader    io.Reader
	LogChan   chan *[]common.LogEntry
	Wg        *sync.WaitGroup
	BatchSize uint32
	// Cancel stops the read with common.ErrReadCancelled when closed; nil never cancels.
	Cancel <-chan struct{}
	// FromOffset is the offset the first entry must have. Zero takes the offset of the
	// first entry as the start, for a reader whose first offset the caller does not know.
	FromOffset common.Offset
	EndOffset  common.Offset
}

type ReadResult struct {
	LastLogOffset common.Offset
	EntriesRead   uint64
}

func (r *ReadResult) Update(result ReadResult) {
	r.LastLogOffset = result.LastLogOffset
	r.EntriesRead = r.EntriesRead + result.EntriesRead
}

func (r *ReadResult) NextOffset() common.Offset {
	return r.LastLogOffset + 1
}

func ReadFile(params ReadFileParams) (ReadResult, error) {
	if params.BatchSize == 0 {
		return ReadResult{}, errore.New("batch size must be greater than zero")
	}
	currentOffset := params.FromOffset
	var offsetFromLogg common.Offset = 0
	if currentOffset > 0 {
		offsetFromLogg = currentOffset - 1
	}
	var entriesRead uint64 = 0
	reader := bufio.NewReader(params.Reader)
	batchCapacity := params.BatchSize
	if batchCapacity > maxBatchPrealloc {
		batchCapacity = maxBatchPrealloc
	}
	logEntries := make([]common.LogEntry, 0, batchCapacity)
	currentBatchInBytes := 0
	// sendBatch hands the batch to the consumer, or gives up if the read is cancelled
	// so an abandoned read never blocks on a batch nobody will receive.
	sendBatch := func() error {
		select {
		case <-params.Cancel:
			return common.ErrReadCancelled
		default:
		}
		params.Wg.Add(1)
		batch := logEntries
		select {
		case params.LogChan <- &batch:
		case <-params.Cancel:
			params.Wg.Done()
			return common.ErrReadCancelled
		}
		logEntries = make([]common.LogEntry, 0, batchCapacity)
		currentBatchInBytes = 0
		return nil
	}
	for {
		if currentOffset == params.EndOffset {
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
		if len(logEntries) == int(params.BatchSize) || currentBatchInBytes > maxBatchBytes {
			if err := sendBatch(); err != nil {
				return ReadResult{}, errore.Wrap(err)
			}
		}
		entry, _, err := common.ReadEntry(reader, common.MaxEntrySize)
		if err == io.EOF {
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
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		offsetFromLogg = common.Offset(entry.Offset)
		if currentOffset == 0 {
			currentOffset = offsetFromLogg
		}
		if currentOffset != offsetFromLogg {
			return ReadResult{}, errore.NewF("read order assertion failed, expected [%d] actual [%d]", currentOffset, offsetFromLogg)
		}
		logEntries = append(logEntries, entry)
		currentBatchInBytes = currentBatchInBytes + entry.ByteSize
		entriesRead = entriesRead + 1
		currentOffset = currentOffset + 1
	}
}

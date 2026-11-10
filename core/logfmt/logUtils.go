// Package logfmt reads and recovers log blocks through the driven.BlockStore port. It knows
// how a block is framed and how an entry is encoded, and nothing about where the bytes live.
package logfmt

import (
	"bufio"
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
	// a producer's write is a group of frames appended together, and a group cut short by a
	// crash is cut whole: it was never acknowledged as durable, and keeping a prefix of it
	// would leave its retry nothing to recognise. group is the one being read; a write without
	// a producer keeps the old rule, any prefix of it survives.
	var group producerGroup
	// a block that starts with zeros where its first header should be is a hole, not a block
	// written before framing: the page cache held a first write the media never got, because
	// its fsync failed and a later one set the size without writing it. Zeros are torn, and
	// cut; a block written before framing starts with the checksum of its first entry, which
	// is zero once in four billion.
	if head, _ := reader.Peek(domain.FrameHeaderSize); len(head) > 0 && allZero(head) {
		return nextOffset, 0, nil
	}
	for {
		// a payload cannot be larger than what is left of the block
		var maxStored uint64
		if remaining := blockSize - validSize - domain.FrameHeaderSize; remaining > 0 {
			maxStored = uint64(remaining)
		}
		header, err := domain.ReadFrameHeader(reader, maxStored)
		if err == io.EOF {
			return group.end(nextOffset, validSize)
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
			return group.end(nextOffset, validSize)
		}
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		err = domain.VerifyFramePayload(reader, header)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, domain.ErrCorruptFrame) {
			return group.end(nextOffset, validSize)
		}
		if err != nil {
			return 0, 0, errore.Wrap(err)
		}
		if header.FirstOffset != nextOffset {
			return 0, 0, errore.NewF("log block %s: frame at byte %d starts at offset %d, expected %d",
				ref, validSize, header.FirstOffset, nextOffset)
		}
		group.add(header, nextOffset, validSize)
		nextOffset = header.EndOffset()
		validSize = validSize + header.Size()
	}
}

// producerGroup follows the frames of one producer's write through a scan.
type producerGroup struct {
	producer domain.ProducerID
	sequence uint64
	want     uint32
	have     uint32
	// startOffset and startSize are where the group began: where a scan ends if it is cut
	startOffset domain.Offset
	startSize   int64
}

// add counts a verified frame into the group it belongs to, starting a new one when it does
// not continue the last.
func (g *producerGroup) add(h domain.FrameHeader, offset domain.Offset, size int64) {
	continues := !h.Producer.IsZero() && h.Producer == g.producer && h.Sequence == g.sequence && g.have < g.want
	if !continues {
		*g = producerGroup{producer: h.Producer, sequence: h.Sequence, want: h.WriteEntries,
			startOffset: offset, startSize: size}
	}
	g.have += h.EntryCount
}

// end is where a scan that stops here ends: after the last frame, unless that frame left a
// producer's write incomplete, in which case before the write.
func (g *producerGroup) end(offset domain.Offset, size int64) (domain.Offset, int64, error) {
	if !g.producer.IsZero() && g.have < g.want {
		return g.startOffset, g.startSize, nil
	}
	return offset, size, nil
}

// ProducerWrite is one producer's write, as the frame headers of a block describe it.
type ProducerWrite struct {
	Producer domain.ProducerID
	Sequence uint64
	First    domain.Offset
	Entries  uint32
}

// ScanDictionaries lists the dictionary ids a log block's frames name, in the order frames
// first name them, from the frame headers alone.
func ScanDictionaries(store driven.BlockStore, ref driven.BlockRef) ([]uint64, error) {
	block, err := store.Open(ref, 0)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	defer block.Close()
	reader := bufio.NewReader(block)
	var ids []uint64
	seen := map[uint64]bool{}
	for {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err == io.EOF {
			return ids, nil
		}
		if err != nil {
			return nil, errore.WrapWithContextF(err, "reading the frame headers of %s", ref)
		}
		if err := domain.SkipFramePayload(reader, header); err != nil {
			return nil, errore.WrapWithContextF(err, "reading the frame headers of %s", ref)
		}
		if header.DictID != 0 && !seen[header.DictID] {
			seen[header.DictID] = true
			ids = append(ids, header.DictID)
		}
	}
}

// ScanProducers lists the producer writes a log block holds, in order, from its frame headers
// alone: nothing is decoded and no payload is read, which is what makes rebuilding the
// producer table after a restart cheap. The block must already have been recovered.
func ScanProducers(store driven.BlockStore, ref driven.BlockRef) ([]ProducerWrite, error) {
	block, err := store.Open(ref, 0)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	defer block.Close()
	reader := bufio.NewReader(block)
	var writes []ProducerWrite
	for {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err == io.EOF {
			return writes, nil
		}
		if err != nil {
			return nil, errore.WrapWithContextF(err, "reading the frame headers of %s", ref)
		}
		if err := domain.SkipFramePayload(reader, header); err != nil {
			return nil, errore.WrapWithContextF(err, "reading the frame headers of %s", ref)
		}
		if header.Producer.IsZero() {
			continue
		}
		if n := len(writes); n > 0 && writes[n-1].Producer == header.Producer &&
			writes[n-1].Sequence == header.Sequence && writes[n-1].First+domain.Offset(writes[n-1].Entries) == header.FirstOffset {
			writes[n-1].Entries += header.EntryCount
			continue
		}
		writes = append(writes, ProducerWrite{Producer: header.Producer, Sequence: header.Sequence,
			First: header.FirstOffset, Entries: header.EntryCount})
	}
}

func allZero(bytes []byte) bool {
	for _, b := range bytes {
		if b != 0 {
			return false
		}
	}
	return true
}

type ReadFileParams struct {
	// Reader holds the log block, positioned at the start of the frame holding FromOffset.
	Reader io.Reader
	// Codecs resolves the codec byte a frame carries. A nil registry reads frames written
	// without compression and reports the rest as unknown.
	Codecs driven.Codecs
	// Dictionaries finds a dictionary a frame names. Nil reads every frame that names none,
	// and reports the rest as a missing dictionary.
	Dictionaries driven.Dictionaries
	LogChan      chan *[]domain.LogEntry
	Wg           *sync.WaitGroup
	BatchSize    uint32
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
		entries, err := DecodeFrameWith(params.Codecs, params.Dictionaries, header, payload)
		if err != nil {
			return ReadResult{}, errore.Wrap(err)
		}
		// the entries handed on alias this frame's buffer, which nothing reuses: one
		// allocation per frame instead of one per entry, and no copy of bytes already read
		inFrame := 0
		for i := uint32(0); i < header.EntryCount; i++ {
			entry, n, err := domain.ParseEntry(entries[inFrame:], domain.MaxEntrySize)
			if err != nil {
				return ReadResult{}, errore.Wrap(err)
			}
			inFrame = inFrame + n
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

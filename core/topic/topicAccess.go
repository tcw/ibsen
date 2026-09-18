package topic

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

type TopicAccess interface {
	UpdateIndex() (bool, error)
	LoadOrCreate() error
	Read(params domain.ReadLogParams) error
	Write(entries domain.EntriesPtr) error
}

var _ TopicAccess = &Topic{}

// ErrTopicClosed is returned by writes to a topic after Close.
var ErrTopicClosed = errors.New("topic is closed")

// DefaultIndexSparsity is the number of entries between two index pairs when a caller does
// not choose one. Denser indexes make a read scan less and cost more bytes and more work per
// write; sparser ones the other way round.
const DefaultIndexSparsity uint32 = 10

// A frame is the smallest thing a read can be aimed at: it is decoded whole, and an index
// pair points at its start and never inside it. So a frame is bounded, and a write larger
// than the bound becomes several frames rather than one big one. Without that, a client
// writing a million entries in one call would produce one frame, which a read would have to
// decode in full to reach any offset in it, and which the index could offer exactly one pair
// for, whatever sparsity was asked for.
//
// The bounds are deliberately generous. They are there to stop a frame growing without
// limit, not to tune it: a frame is also the unit a codec gets to find redundancy in, and
// cutting it small costs compression.
const (
	// DefaultMaxFrameEntries is how many entries may share a frame.
	DefaultMaxFrameEntries uint32 = 1000
	// DefaultMaxFrameBytes is how many encoded entry bytes may share a frame, before the
	// codec sees them. One entry larger than this still gets a frame of its own.
	DefaultMaxFrameBytes int = 1 << 20
)

// indexPairSize is the bytes one pair takes in an index block. The index package owns the
// encoding; this is here so the truncation arithmetic reads in terms of pairs.
const indexPairSize = index.PairSize

type Topic struct {
	mu        sync.RWMutex
	Store     driven.BlockStore
	Log       driven.Logger
	TopicName string
	// indexing is 1 while a run is under way; indexPending is the mark a caller leaves when
	// it finds one, and the run takes that mark before it stops. Together they coalesce
	// indexing the way the flusher coalesces syncing, so work is never dropped and never
	// waits for a timer.
	indexing        int32
	indexPending    int32
	indexWg         *sync.WaitGroup
	MaxBlockSize    int
	IndexSparsity   uint32
	MaxFrameEntries uint32
	MaxFrameBytes   int
	// Codec compresses the frames this topic writes; Codecs resolves the codec byte of
	// frames already written, which may name one this topic no longer writes with.
	Codec  driven.Codec
	Codecs driven.Codecs
	// flush decides when appended entries are durable, and is what a read is bounded by
	flush          *flusher
	NextOffset     domain.Offset
	HeadBlockSize  int
	LogBlockList   []domain.LogBlock
	IndexBlockList []domain.IndexBlock
	IndexPosition  *domain.LogBlockPosition
	// writeFailure is set when a failed write could not be rolled back. The head block may
	// end in a partial entry, so writes are refused until LoadOrCreate recovers it.
	writeFailure error
	// closed is set by Close; writes are refused so no new background indexing starts
	closed bool
}

// Params is what the core needs to open one topic: the driven store to keep its blocks in,
// the topic's name and the size at which it rolls over to a new block.
type Params struct {
	Store        driven.BlockStore
	TopicName    string
	MaxBlockSize int
	// IndexSparsity is the number of entries between two index pairs. Zero means
	// DefaultIndexSparsity. Changing it between runs is safe: the pairs already written stay
	// valid and sorted, and the block simply ends up indexed at two densities.
	IndexSparsity uint32
	// Codec compresses the frames this topic writes. Nil writes them uncompressed, which
	// every build can read back. Changing it between runs is safe: a frame carries the byte
	// naming what it was written with, so a block may hold frames of several codecs.
	Codec driven.Codec
	// Codecs resolves that byte when a frame is read. Nil reads uncompressed frames and
	// reports anything else as an unknown codec rather than as damage.
	Codecs driven.Codecs
	// MaxFrameEntries and MaxFrameBytes bound one frame; a write larger than either becomes
	// several. Zero means DefaultMaxFrameEntries and DefaultMaxFrameBytes.
	MaxFrameEntries uint32
	MaxFrameBytes   int
	// FlushEntries is how many entries may wait before a flush is forced. Zero means
	// DefaultFlushEntries, which is 1: every write is durable before it is acknowledged.
	FlushEntries uint32
	// FlushInterval is how long a batch may be held back hoping for more entries. Zero
	// never holds one back. A store that cannot sync ignores both.
	FlushInterval time.Duration
	// Logger is optional: a core built without one logs nothing rather than crashing.
	Logger driven.Logger
}

func NewLogTopic(params Params) *Topic {
	logger := params.Logger
	if logger == nil {
		logger = driven.NopLogger{}
	}
	sparsity := params.IndexSparsity
	if sparsity == 0 {
		sparsity = DefaultIndexSparsity
	}
	codec := params.Codec
	if codec == nil {
		codec = driven.NoCodec{}
	}
	codecs := params.Codecs
	if codecs == nil {
		codecs = driven.NewCodecs()
	}
	maxFrameEntries := params.MaxFrameEntries
	if maxFrameEntries == 0 {
		maxFrameEntries = DefaultMaxFrameEntries
	}
	maxFrameBytes := params.MaxFrameBytes
	if maxFrameBytes == 0 {
		maxFrameBytes = DefaultMaxFrameBytes
	}
	return &Topic{
		Store:           params.Store,
		Log:             logger,
		IndexSparsity:   sparsity,
		MaxFrameEntries: maxFrameEntries,
		MaxFrameBytes:   maxFrameBytes,
		Codec:           codec,
		Codecs:          codecs,
		flush:           newFlusher(params.Store, params.FlushEntries, params.FlushInterval),
		TopicName:       params.TopicName,
		indexWg:         &sync.WaitGroup{},
		NextOffset:      0,
		HeadBlockSize:   0,
		MaxBlockSize:    params.MaxBlockSize,
		LogBlockList:    []domain.LogBlock{},
		IndexBlockList:  []domain.IndexBlock{},
		IndexPosition:   nil,
	}
}

// topic is the name the store knows this topic by.
func (t *Topic) topic() domain.TopicName {
	return domain.TopicName(t.TopicName)
}

func (t *Topic) logRef(block domain.LogBlock) driven.BlockRef {
	return driven.LogRef(t.topic(), block)
}

func (t *Topic) indexRef(block domain.IndexBlock) driven.BlockRef {
	return driven.IndexRef(t.topic(), block)
}

// UpdateIndex brings the index up to date with the log, and reports whether this call was the
// one that did it. False means another call was already running; that call takes on this
// one's work too, so the answer is "somebody is indexing", not "this was dropped".
//
// A caller that finds a run under way leaves a mark rather than its work, and the run takes
// the mark before it stops. That closes the window the old code left: it released the
// exclusion flag after the topic lock, so a write landing in between had its index request
// thrown away, and nothing but a ten-second sweep over every loaded topic would notice. There
// is no sweep now, and so no timer for an embedded build to carry.
//
// This is the flusher's arrangement applied to indexing (§2): whoever is already working
// takes the work that arrives while it works, rather than everyone re-deciding.
func (t *Topic) UpdateIndex() (bool, error) {
	// the mark goes up first, so a run on its way out sees it before it decides to stop
	atomic.StoreInt32(&t.indexPending, 1)
	if !atomic.CompareAndSwapInt32(&t.indexing, 0, 1) {
		t.Log.Log(driven.LevelDebug, "index already running, left it the work")
		return false, nil
	}
	for {
		for atomic.SwapInt32(&t.indexPending, 0) == 1 {
			if err := t.indexOnce(); err != nil {
				// the work is still outstanding, so put the mark back for the next write or
				// the next explicit call. Retrying here would spin on a store that is failing,
				// which is the rule a failed flush follows too.
				atomic.StoreInt32(&t.indexPending, 1)
				atomic.StoreInt32(&t.indexing, 0)
				return true, err
			}
		}
		atomic.StoreInt32(&t.indexing, 0)
		// a caller that arrived between taking the mark and releasing the flag left it for
		// someone who was already leaving. Take it, rather than let the tail of the log go
		// unindexed until the next write happens along.
		if atomic.LoadInt32(&t.indexPending) == 0 {
			return true, nil
		}
		if !atomic.CompareAndSwapInt32(&t.indexing, 0, 1) {
			// somebody else got there first and now owns the mark
			return true, nil
		}
	}
}

// indexOnce brings the index up to date with the log as it stands now. It holds the topic
// lock throughout, so no write lands in the middle of a scan.
func (t *Topic) indexOnce() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// index log blocks not already indexed
	notIndexed, err := t.findBlocksToIndex()
	if err == domain.NoBlocksFound {
		return nil
	}
	if err != nil {
		return errore.Wrap(err)
	}

	for _, block := range notIndexed {
		// if no blocks have been indexed
		if t.IndexPosition == nil {
			pos, err := t.indexBlock(block, 0)
			if err != nil {
				return errore.Wrap(err)
			}
			t.debugLogIndexing(pos.Block, true, "first block")
			t.addNewIndexBlock(block)
			t.IndexPosition = &pos
			continue
		}
		position := t.IndexPosition
		// if indexing a block which is partly indexed
		if position.Block == block {
			pos, err := t.indexBlock(block, position.ByteOffset)
			if err != nil {
				return errore.Wrap(err)
			}
			t.debugLogIndexing(pos.Block, pos.ByteOffset == position.ByteOffset, "existing block")
			t.IndexPosition = &pos
			continue
		}
		// indexing a new block after start block
		pos, err := t.indexBlock(block, 0)
		t.debugLogIndexing(pos.Block, true, "new block")
		if err != nil {
			return errore.Wrap(err)
		}
		t.addNewIndexBlock(block)
		t.IndexPosition = &pos
	}
	return nil
}

func (t *Topic) LoadOrCreate() error {
	if err := domain.ValidateTopicName(t.topic()); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	created, err := t.Store.CreateTopic(t.topic())
	if err != nil {
		return errore.Wrap(err)
	}
	if created {
		return nil
	}
	// Load log and index blocks from the store
	logBlocks, err := t.Store.List(t.topic(), driven.Log)
	if err != nil {
		return errore.Wrap(err)
	}
	indexBlocks, err := t.Store.List(t.topic(), driven.Index)
	if err != nil {
		return errore.Wrap(err)
	}
	// a topic without blocks has never been written to
	if len(logBlocks) == 0 {
		return nil
	}
	t.LogBlockList = toLogBlocks(logBlocks)
	t.IndexBlockList = toIndexBlocks(indexBlocks)

	// Find the end of the log, truncating a torn tail left by an interrupted write
	head := logBlocks[len(logBlocks)-1]
	nextOffset, validSize, truncated, err := logfmt.RecoverBlock(t.Store,
		t.logRef(domain.LogBlock(head.Block)), domain.Offset(head.Block), head.Size)
	if err != nil {
		return errore.Wrap(err)
	}
	if truncated > 0 {
		t.Log.Log(driven.LevelWarn, "truncated torn tail of log block",
			driven.Str("topic", t.TopicName),
			driven.Uint64("logBlock", head.Block),
			driven.Int64("truncatedBytes", truncated))
	}
	t.NextOffset = nextOffset
	t.flush.reset(nextOffset)
	t.HeadBlockSize = int(validSize)
	t.writeFailure = nil

	// Find position of last entry write to index
	position, err := t.findCurrentIndexLogBlockPosition()
	if err != nil {
		return errore.Wrap(err)
	}
	t.IndexPosition = position
	t.debugLogLoadResult(logBlocks, indexBlocks)
	return nil
}

func toLogBlocks(blocks []driven.Block) []domain.LogBlock {
	list := make([]domain.LogBlock, 0, len(blocks))
	for _, block := range blocks {
		list = append(list, domain.LogBlock(block.Block))
	}
	return list
}

func toIndexBlocks(blocks []driven.Block) []domain.IndexBlock {
	list := make([]domain.IndexBlock, 0, len(blocks))
	for _, block := range blocks {
		list = append(list, domain.IndexBlock(block.Block))
	}
	return list
}

// ReadLog
// Reads a log from and including the ReadLogParams.From offset until end of log.
func (t *Topic) Read(params domain.ReadLogParams) error {
	if err := domain.ValidateTopicName(t.topic()); err != nil {
		return err
	}
	return t.snapshot().read(params)
}

// snapshot copies the topic state so a read does not hold the lock while
// blocked on a slow consumer.
func (t *Topic) snapshot() *Topic {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return &Topic{
		Store:           t.Store,
		Log:             t.Log,
		TopicName:       t.TopicName,
		MaxBlockSize:    t.MaxBlockSize,
		IndexSparsity:   t.IndexSparsity,
		MaxFrameEntries: t.MaxFrameEntries,
		MaxFrameBytes:   t.MaxFrameBytes,
		Codec:           t.Codec,
		Codecs:          t.Codecs,
		flush:           t.flush,
		NextOffset:      t.NextOffset,
		HeadBlockSize:   t.HeadBlockSize,
		LogBlockList:    append([]domain.LogBlock(nil), t.LogBlockList...),
		IndexBlockList:  append([]domain.IndexBlock(nil), t.IndexBlockList...),
		IndexPosition:   t.IndexPosition,
	}
}

func (t *Topic) read(params domain.ReadLogParams) error {
	// ensures reader will not read partially written log entries
	endOffset, exists := t.findLastConfirmedWrittenEntryOffset(params.From)
	if !exists {
		return domain.NoEntriesFound
	}
	block, found := t.logBlockContaining(params.From)
	if !found {
		return errore.New("offset out of bounds, this should never happen!")
	}

	// find byte offset in the block to start reading from
	byteOffset, scanCount, err := t.findByteOffsetInLogBlock(params.From)
	if err == logfmt.NoByteOffsetFound {
		return domain.NoEntriesFound
	}
	if err != nil {
		return errore.Wrap(err)
	}
	t.debugLogIndexLookup(params.From, byteOffset, scanCount)

	if err = t.sendBlock(t.logRef(block), byteOffset, params.From, endOffset, params); err != nil {
		return errore.Wrap(err)
	}

	// read remaining log blocks
	wasFound, i := t.findBlockArrayIndex(block)
	if !wasFound {
		return nil
	}
	for _, b := range t.LogBlockList[i+1:] {
		endOffset, _ = t.endBoundaryForReadOffset()
		err = t.sendBlock(t.logRef(b), 0, domain.Offset(b), endOffset, params)
		if errors.Is(err, driven.ErrBlockNotFound) {
			break
		}
		if err != nil {
			return errore.Wrap(err)
		}
	}
	return nil
}

// sendBlock reads one log block from byteOffset and sends its entries to the consumer.
func (t *Topic) sendBlock(ref driven.BlockRef, byteOffset int64, from domain.Offset, endOffset domain.Offset, params domain.ReadLogParams) error {
	blockReader, err := t.Store.Open(ref, byteOffset)
	if err != nil {
		return err
	}
	_, err = logfmt.ReadFile(logfmt.ReadFileParams{
		Reader:     blockReader,
		Codecs:     t.Codecs,
		LogChan:    params.LogChan,
		Wg:         params.Wg,
		BatchSize:  params.BatchSize,
		Cancel:     params.Cancel,
		FromOffset: from,
		EndOffset:  endOffset,
	})
	t.closeBlock(ref, blockReader)
	return err
}

// Write appends entries and returns once they are on durable media. The offsets it wrote
// only become readable at that point, so an acknowledged write and a readable write are the
// same thing.
func (t *Topic) Write(entries domain.EntriesPtr) error {
	if err := domain.ValidateTopicName(t.topic()); err != nil {
		return err
	}
	pending, err := t.append(entries)
	if err != nil {
		return err
	}
	// waiting happens outside t.mu: a sync can be slow, and readers take that lock
	return t.flush.wait(pending)
}

// append writes entries into the head block and returns the flush that will make them
// durable, or nil when the store has nothing to flush.
func (t *Topic) append(entries domain.EntriesPtr) (*pendingFlush, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writeFailure != nil {
		return nil, errore.Wrap(t.writeFailure)
	}
	if t.closed {
		return nil, ErrTopicClosed
	}

	// if topic is empty create the first log block
	if t.logBlockIsEmpty() {
		t.addNewLogBlock()
	}
	// if block has excited is max size create a new block
	if t.HeadBlockSize > t.MaxBlockSize {
		t.addNewLogBlock()
		t.resetHeadBlockSize()
	}
	// encode the entries into frames and append them to the head block
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return nil, errors.New("Topic " + t.TopicName + " has no block head")
	}
	frames, offsets, err := t.buildFrames(entries)
	if err != nil {
		return nil, errore.Wrap(err)
	}

	block, err := t.Store.Append(t.logRef(head), frames)
	if err != nil {
		// the store could not undo a partial append, so the head block has to be recovered
		// before anything is written to it again
		if errors.Is(err, driven.ErrDirtyBlock) {
			t.writeFailure = err
		}
		return nil, errore.Wrap(err)
	}

	// update internal log state
	t.incrementOffset(offsets)
	t.HeadBlockSize = int(block.Size)
	pending := t.flush.appended(t.logRef(head), t.NextOffset, offsets)

	// update index async if no index is running; Close waits for it. The Add happens under
	// t.mu before closed is set, so it never races with the Wait in Close.
	t.indexWg.Add(1)
	go func() {
		defer t.indexWg.Done()
		wasExecuted, err := t.UpdateIndex()
		if err != nil {
			t.Log.Log(driven.LevelWarn, "background index update failed",
				driven.Err(err), driven.Str("topic", t.TopicName))
		}
		t.Log.Log(driven.LevelTrace, "index update executed", driven.Bool("executed", wasExecuted))
	}()
	return pending, nil
}

// Close refuses further writes and waits for the background indexing started by earlier
// writes. It does not wait for UpdateIndex calls made by others; stop those first.
func (t *Topic) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.indexWg.Wait()
	// every writer waits for its own batch, so the only entries left unflushed are those a
	// failed flush put back; try once more before the topic goes away
	t.flush.flushRemaining()
}

func (t *Topic) debugLogLoadResult(logBlocks []driven.Block, indexBlocks []driven.Block) {
	if !t.Log.Enabled(driven.LevelDebug) {
		return
	}
	t.Log.Log(driven.LevelDebug, "loaded topic",
		driven.Str("topic", t.TopicName),
		driven.Int("logBlocks", len(logBlocks)),
		driven.Int("indexBlocks", len(indexBlocks)),
		driven.Int("nextOffset", int(t.NextOffset)),
		driven.Int("headBlockSize", t.HeadBlockSize))
}

func (t *Topic) debugLogIndexing(logBlock domain.LogBlock, indexUpdated bool, posDesc string) {
	if !t.Log.Enabled(driven.LevelDebug) {
		return
	}
	t.Log.Log(driven.LevelDebug, "index on "+posDesc,
		driven.Str("topic", t.TopicName),
		driven.Uint64("logBlock", uint64(logBlock)),
		driven.Int64("byteOffset", 0),
		driven.Bool("index_updated", indexUpdated))
}

func (t *Topic) closeBlock(ref driven.BlockRef, block io.Closer) {
	if block == nil {
		return
	}
	if err := block.Close(); err != nil {
		t.Log.Log(driven.LevelWarn, "unable to close block", driven.Str("block", ref.String()))
	}
}

// readBlock returns everything a block holds.
func (t *Topic) readBlock(ref driven.BlockRef) ([]byte, error) {
	block, err := t.Store.Open(ref, 0)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(block)
	t.closeBlock(ref, block)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return content, nil
}

func (t *Topic) findLastConfirmedWrittenEntryOffset(from domain.Offset) (domain.Offset, bool) {
	if t.logBlockIsEmpty() {
		return 0, false
	}
	endOffset, hasEnd := t.endBoundaryForReadOffset()
	if !hasEnd {
		return 0, false
	}
	if from >= endOffset {
		return 0, false
	}
	return endOffset, true
}

func (t *Topic) ToString() string {
	list := t.LogBlockList
	blocklist := ""
	for i, val := range list {
		blocklist = blocklist + fmt.Sprintf("%d -> %d\n", i, val)
	}
	return blocklist
}

// buildFrames encodes entries into one frame, or into several when the write is larger than
// a frame may be. They are appended in one call, so the store still sees a write whole or
// not at all and a flush still never lands inside a frame; what changes is only that a large
// write is addressable at more than one point.
func (t *Topic) buildFrames(entries domain.EntriesPtr) ([]byte, int, error) {
	var frames []byte
	var payload []byte
	firstOffset := t.NextOffset
	inFrame := 0
	written := 0
	// closeFrame encodes what has been collected and starts the next frame after it
	closeFrame := func() error {
		if inFrame == 0 {
			return nil
		}
		frame, err := logfmt.EncodeFrame(t.Codec, firstOffset, inFrame, payload)
		if err != nil {
			return err
		}
		frames = append(frames, frame...)
		firstOffset = t.NextOffset + domain.Offset(written)
		payload = payload[:0]
		inFrame = 0
		return nil
	}
	for _, entry := range *entries {
		encoded := domain.CreateByteEntry(entry, t.NextOffset+domain.Offset(written))
		// an entry larger than the byte bound still gets a frame, its own
		if inFrame > 0 && (uint32(inFrame) >= t.MaxFrameEntries || len(payload)+len(encoded) > t.MaxFrameBytes) {
			if err := closeFrame(); err != nil {
				return nil, 0, err
			}
		}
		payload = append(payload, encoded...)
		written = written + 1
		inFrame = inFrame + 1
	}
	if err := closeFrame(); err != nil {
		return nil, 0, err
	}
	return frames, written, nil
}

// endBoundaryForReadOffset is where a read stops: the newest entry known to be on durable
// media, which trails NextOffset by whatever has been appended but not yet flushed. Readers
// never see an entry a power cut could take back.
func (t *Topic) endBoundaryForReadOffset() (domain.Offset, bool) {
	durable := t.durableOffset()
	if durable == 0 {
		return 0, false
	}
	return durable, true
}

// durableOffset is the offset after the newest durable entry. A topic built without a
// flusher has nothing buffered, so everything written counts as durable.
func (t *Topic) durableOffset() domain.Offset {
	if t.flush == nil {
		return t.NextOffset
	}
	return t.flush.durableOffset()
}

func (t *Topic) indexBlock(block domain.LogBlock, byteOffset int64) (domain.LogBlockPosition, error) {
	logBlock, err := t.Store.Open(t.logRef(block), byteOffset)
	if err != nil {
		return domain.LogBlockPosition{}, errore.Wrap(err)
	}
	indexAsBytes, newByteOffset, err := index.CreateBinaryIndexFromLog(logBlock, byteOffset, t.IndexSparsity)
	t.closeBlock(t.logRef(block), logBlock)
	if err != nil {
		return domain.LogBlockPosition{}, errore.Wrap(err)
	}
	// an append of no pairs still creates the index block, so a reload finds it beside its
	// log block even when the block holds no offset worth indexing yet
	if _, err = t.Store.Append(t.indexRef(domain.IndexBlock(block)), indexAsBytes); err != nil {
		return domain.LogBlockPosition{}, errore.Wrap(err)
	}
	return domain.LogBlockPosition{
		Block:      block,
		ByteOffset: newByteOffset,
	}, nil
}

// findCurrentIndexLogBlockPosition returns where indexing resumes: right after the last
// indexed entry of the newest index block. It first drops index pairs that point past the
// recovered end of the log and any torn trailing bytes, so later appends stay aligned.
func (t *Topic) findCurrentIndexLogBlockPosition() (*domain.LogBlockPosition, error) {
	indexBlockHead, hasBlock := t.indexBlockHead()
	if !hasBlock {
		return nil, nil
	}
	indexRef := t.indexRef(indexBlockHead)
	byteIndex, err := t.readBlock(indexRef)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	kept := index.NewIndex(byteIndex).IndexOffsets
	for len(kept) > 0 && kept[len(kept)-1].Offset >= t.NextOffset {
		kept = kept[:len(kept)-1]
	}
	position := &domain.LogBlockPosition{Block: domain.LogBlock(indexBlockHead)}
	if len(kept) > 0 {
		last := kept[len(kept)-1]
		header, err := logfmt.ReadFrameHeaderAt(t.Store, t.logRef(domain.LogBlock(indexBlockHead)), last.ByteOffset)
		// the frame must still be there, still start where the pair says, and still end
		// inside the recovered log: a frame whose tail was cut is not one to resume after
		if err != nil || header.FirstOffset != last.Offset || header.EndOffset() > t.NextOffset {
			// the index does not match the log, rebuild this block's index from the start
			kept = nil
		} else {
			position.ByteOffset = last.ByteOffset + header.Size()
		}
	}
	// the kept pairs are a prefix of the block, so dropping the rest is a truncation
	if len(byteIndex) != len(kept)*indexPairSize {
		if err = t.Store.Truncate(indexRef, int64(len(kept)*indexPairSize)); err != nil {
			return nil, errore.Wrap(err)
		}
	}
	return position, nil
}

func (t *Topic) findByteOffsetInLogBlock(offset domain.Offset) (int64, int, error) {
	indexBlock, foundIndexBlock := t.indexBlockContaining(offset)
	if offset >= t.NextOffset {
		return 0, 0, errors.New("offset out of bounds")
	}
	logBlock, logBlockFound := t.logBlockContaining(offset)
	if !logBlockFound {
		return 0, 0, errors.New("no log block containing offset found")
	}
	if uint64(logBlock) == uint64(offset) {
		return 0, 0, nil
	}
	// byte offsets in an index are only valid for its own log block
	if !foundIndexBlock || uint64(indexBlock) != uint64(logBlock) {
		return logfmt.FindFrameByteOffset(t.Store, t.logRef(logBlock), 0, offset)
	}
	idx, err := t.getIndexFromIndexBlock(indexBlock)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	if idx == nil {
		return logfmt.FindFrameByteOffset(t.Store, t.logRef(logBlock), 0, offset)
	}
	indexOffset := idx.FindNearestByteOffset(offset)
	if indexOffset.Offset > offset {
		return 0, 0, errore.NewF("found larger offset than upper bound")
	}
	if indexOffset.Offset == offset {
		return indexOffset.ByteOffset, 0, nil
	}

	return logfmt.FindFrameByteOffset(t.Store, t.logRef(logBlock), indexOffset.ByteOffset, offset)
}

func (t *Topic) getIndexFromIndexBlock(block domain.IndexBlock) (*index.Index, error) {
	bytes, err := t.readBlock(t.indexRef(block))
	if errors.Is(err, driven.ErrBlockNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return index.NewIndex(bytes), nil
}

func (t *Topic) incrementOffset(n int) {
	t.NextOffset = t.NextOffset + domain.Offset(n)
}

func (t *Topic) resetHeadBlockSize() {
	t.HeadBlockSize = 0
}

func (t *Topic) addNewLogBlock() {
	t.LogBlockList = append(t.LogBlockList, domain.LogBlock(t.NextOffset))
}

func (t *Topic) addNewIndexBlock(logBlock domain.LogBlock) {
	t.IndexBlockList = append(t.IndexBlockList, domain.IndexBlock(logBlock))
}

func (t *Topic) logBlockHead() (domain.LogBlock, bool) {
	if t.logBlockIsEmpty() {
		return 0, false
	}
	return t.LogBlockList[len(t.LogBlockList)-1], true
}

func (t *Topic) indexBlockHead() (domain.IndexBlock, bool) {
	if t.logBlockIsEmpty() {
		return 0, false
	}
	if len(t.IndexBlockList) == 0 {
		return 0, false
	}
	return t.IndexBlockList[len(t.IndexBlockList)-1], true
}

func (t *Topic) logBlockIsEmpty() bool {
	return len(t.LogBlockList) == 0
}

func (t *Topic) logSize() int {
	return len(t.LogBlockList)
}

func (t *Topic) indexSize() int {
	return len(t.IndexBlockList)
}

func (t *Topic) findBlocksToIndex() ([]domain.LogBlock, error) {
	if t.logSize() == 0 {
		return nil, domain.NoBlocksFound
	}
	logStartPos := t.indexSize() - 1
	if logStartPos < 0 {
		logStartPos = 0
	}
	return t.LogBlockList[logStartPos:], nil
}

func (t *Topic) indexBlockContaining(offset domain.Offset) (domain.IndexBlock, bool) {
	if t.indexSize() == 0 {
		return 0, false
	}
	for i := t.indexSize() - 1; i >= 0; i-- {
		if offset >= domain.Offset(t.IndexBlockList[i]) {
			return t.IndexBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) logBlockContaining(offset domain.Offset) (domain.LogBlock, bool) {
	if t.logSize() == 0 {
		return 0, false
	}
	if t.NextOffset <= offset {
		return 0, false
	}
	if t.logSize() == 1 {
		return t.LogBlockList[0], true
	}
	for i := t.logSize() - 1; i >= 0; i-- {
		if offset >= domain.Offset(t.LogBlockList[i]) {
			return t.LogBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) findBlockArrayIndex(block domain.LogBlock) (bool, int) {
	for i, b := range t.LogBlockList {
		if b == block {
			return true, i
		}
	}
	return false, 0
}

func (t *Topic) debugLogIndexLookup(from domain.Offset, byteOffset int64, scanCount int) {
	if !t.Log.Enabled(driven.LevelDebug) {
		return
	}
	t.Log.Log(driven.LevelDebug, "read log - index scan count",
		driven.Str("topic", t.TopicName),
		driven.Uint64("fromOffset", uint64(from)),
		driven.Int64("FoundByteOffset", byteOffset),
		driven.Int("scanned", scanCount))
}

package access

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
	ibsLog "github.com/tcw/ibsen/access/log"
	"github.com/tcw/ibsen/errore"
)

type TopicAccess interface {
	UpdateIndex() (bool, error)
	LoadOrCreate() error
	Read(params common.ReadLogParams) error
	Write(entries common.EntriesPtr) error
}

var _ TopicAccess = &Topic{}

// ErrTopicClosed is returned by writes to a topic after Close.
var ErrTopicClosed = errors.New("topic is closed")

// indexSparsity is the number of entries between two index pairs.
const indexSparsity = 10

// indexPairSize is the bytes one (offset, byteOffset) pair takes in an index block.
const indexPairSize = 16

type Topic struct {
	mu             sync.RWMutex
	Store          common.BlockStore
	TopicName      string
	indexMutex     int32
	indexWg        *sync.WaitGroup
	MaxBlockSize   int
	NextOffset     common.Offset
	HeadBlockSize  int
	LogBlockList   []common.LogBlock
	IndexBlockList []common.IndexBlock
	IndexPosition  *common.LogBlockPosition
	// writeFailure is set when a failed write could not be rolled back. The head block may
	// end in a partial entry, so writes are refused until LoadOrCreate recovers it.
	writeFailure error
	// closed is set by Close; writes are refused so no new background indexing starts
	closed bool
}

func NewLogTopic(params common.TopicParams) *Topic {
	return &Topic{
		Store:          params.Store,
		TopicName:      params.TopicName,
		indexWg:        &sync.WaitGroup{},
		NextOffset:     0,
		HeadBlockSize:  0,
		MaxBlockSize:   params.MaxBlockSize,
		LogBlockList:   []common.LogBlock{},
		IndexBlockList: []common.IndexBlock{},
		IndexPosition:  nil,
	}
}

// topic is the name the store knows this topic by.
func (t *Topic) topic() common.TopicName {
	return common.TopicName(t.TopicName)
}

func (t *Topic) logRef(block common.LogBlock) common.BlockRef {
	return common.LogRef(t.topic(), block)
}

func (t *Topic) indexRef(block common.IndexBlock) common.BlockRef {
	return common.IndexRef(t.topic(), block)
}

func (t *Topic) UpdateIndex() (bool, error) {

	// Check if an index is currently running
	if !atomic.CompareAndSwapInt32(&t.indexMutex, 0, 1) {
		log.Debug().Msg("competing indices")
		return false, nil
	}
	defer atomic.CompareAndSwapInt32(&t.indexMutex, 1, 0)
	t.mu.Lock()
	defer t.mu.Unlock()

	// index log blocks not already indexed
	notIndexed, err := t.findBlocksToIndex()
	if err == common.NoBlocksFound {
		return false, nil
	}
	if err != nil {
		return false, errore.Wrap(err)
	}

	for _, block := range notIndexed {
		// if no blocks have been indexed
		if t.IndexPosition == nil {
			pos, err := t.indexBlock(block, 0)
			if err != nil {
				return true, errore.Wrap(err)
			}
			debugLogIndexing(t.TopicName, pos.Block, true, "first block")
			t.addNewIndexBlock(block)
			t.IndexPosition = &pos
			continue
		}
		position := t.IndexPosition
		// if indexing a block which is partly indexed
		if position.Block == block {
			pos, err := t.indexBlock(block, position.ByteOffset)
			if err != nil {
				return true, errore.Wrap(err)
			}
			debugLogIndexing(t.TopicName, pos.Block, pos.ByteOffset == position.ByteOffset, "existing block")
			t.IndexPosition = &pos
			continue
		}
		// indexing a new block after start block
		pos, err := t.indexBlock(block, 0)
		debugLogIndexing(t.TopicName, pos.Block, true, "new block")
		if err != nil {
			return true, errore.Wrap(err)
		}
		t.addNewIndexBlock(block)
		t.IndexPosition = &pos
	}
	return true, nil
}

func (t *Topic) LoadOrCreate() error {
	if err := common.ValidateTopicName(t.topic()); err != nil {
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
	logBlocks, err := t.Store.List(t.topic(), common.Log)
	if err != nil {
		return errore.Wrap(err)
	}
	indexBlocks, err := t.Store.List(t.topic(), common.Index)
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
	nextOffset, validSize, truncated, err := ibsLog.RecoverBlock(t.Store,
		t.logRef(common.LogBlock(head.Block)), common.Offset(head.Block), head.Size)
	if err != nil {
		return errore.Wrap(err)
	}
	if truncated > 0 {
		log.Warn().
			Str("topic", t.TopicName).
			Uint64("logBlock", head.Block).
			Int64("truncatedBytes", truncated).
			Msg("truncated torn tail of log block")
	}
	t.NextOffset = nextOffset
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

func toLogBlocks(blocks []common.Block) []common.LogBlock {
	list := make([]common.LogBlock, 0, len(blocks))
	for _, block := range blocks {
		list = append(list, common.LogBlock(block.Block))
	}
	return list
}

func toIndexBlocks(blocks []common.Block) []common.IndexBlock {
	list := make([]common.IndexBlock, 0, len(blocks))
	for _, block := range blocks {
		list = append(list, common.IndexBlock(block.Block))
	}
	return list
}

// ReadLog
// Reads a log from and including the ReadLogParams.From offset until end of log.
func (t *Topic) Read(params common.ReadLogParams) error {
	if err := common.ValidateTopicName(t.topic()); err != nil {
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
		Store:          t.Store,
		TopicName:      t.TopicName,
		MaxBlockSize:   t.MaxBlockSize,
		NextOffset:     t.NextOffset,
		HeadBlockSize:  t.HeadBlockSize,
		LogBlockList:   append([]common.LogBlock(nil), t.LogBlockList...),
		IndexBlockList: append([]common.IndexBlock(nil), t.IndexBlockList...),
		IndexPosition:  t.IndexPosition,
	}
}

func (t *Topic) read(params common.ReadLogParams) error {
	// ensures reader will not read partially written log entries
	endOffset, exists := t.findLastConfirmedWrittenEntryOffset(params.From)
	if !exists {
		return common.NoEntriesFound
	}
	block, found := t.logBlockContaining(params.From)
	if !found {
		return errore.New("offset out of bounds, this should never happen!")
	}

	// find byte offset in the block to start reading from
	byteOffset, scanCount, err := t.findByteOffsetInLogBlock(params.From)
	if err == ibsLog.NoByteOffsetFound {
		return common.NoEntriesFound
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
		err = t.sendBlock(t.logRef(b), 0, common.Offset(b), endOffset, params)
		if errors.Is(err, common.ErrBlockNotFound) {
			break
		}
		if err != nil {
			return errore.Wrap(err)
		}
	}
	return nil
}

// sendBlock reads one log block from byteOffset and sends its entries to the consumer.
func (t *Topic) sendBlock(ref common.BlockRef, byteOffset int64, from common.Offset, endOffset common.Offset, params common.ReadLogParams) error {
	blockReader, err := t.Store.Open(ref, byteOffset)
	if err != nil {
		return err
	}
	_, err = ibsLog.ReadFile(ibsLog.ReadFileParams{
		Reader:     blockReader,
		LogChan:    params.LogChan,
		Wg:         params.Wg,
		BatchSize:  params.BatchSize,
		Cancel:     params.Cancel,
		FromOffset: from,
		EndOffset:  endOffset,
	})
	closeBlock(ref, blockReader)
	return err
}

func (t *Topic) Write(entries common.EntriesPtr) error {
	if err := common.ValidateTopicName(t.topic()); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writeFailure != nil {
		return errore.Wrap(t.writeFailure)
	}
	if t.closed {
		return ErrTopicClosed
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
	// create a byte representation of entries and append it to the head block
	bytes, offsets := t.buildBinaryEntryRepresentation(entries)
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errors.New("Topic " + t.TopicName + " has no block head")
	}

	block, err := t.Store.Append(t.logRef(head), bytes)
	if err != nil {
		// the store could not undo a partial append, so the head block has to be recovered
		// before anything is written to it again
		if errors.Is(err, common.ErrDirtyBlock) {
			t.writeFailure = err
		}
		return errore.Wrap(err)
	}

	// update internal log state
	t.incrementOffset(offsets)
	t.HeadBlockSize = int(block.Size)

	// update index async if no index is running; Close waits for it. The Add happens under
	// t.mu before closed is set, so it never races with the Wait in Close.
	t.indexWg.Add(1)
	go func() {
		defer t.indexWg.Done()
		wasExecuted, err := t.UpdateIndex()
		if err != nil {
			log.Warn().Err(err).Str("topic", t.TopicName).Msg("background index update failed")
		}
		log.Trace().Msg(fmt.Sprintf("index update executed: %t", wasExecuted))
	}()
	return nil
}

// Close refuses further writes and waits for the background indexing started by earlier
// writes. It does not wait for UpdateIndex calls made by others; stop those first.
func (t *Topic) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.indexWg.Wait()
}

func (t *Topic) debugLogLoadResult(logBlocks []common.Block, indexBlocks []common.Block) {
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Int("logBlocks", len(logBlocks)).
			Int("indexBlocks", len(indexBlocks)).
			Int("nextOffset", int(t.NextOffset)).
			Int("headBlockSize", t.HeadBlockSize).
			Msg("loaded topic")
	}
}

func debugLogIndexing(topicName string, logBlock common.LogBlock, indexUpdated bool, posDesc string) {
	if d := log.Debug(); d.Enabled() {
		d.Str("topic", topicName).
			Uint64("logBlock", uint64(logBlock)).
			Int64("byteOffset", 0).
			Bool("index_updated", indexUpdated).
			Msgf("index on %s", posDesc)
	}
}

func closeBlock(ref common.BlockRef, block io.Closer) {
	if block == nil {
		return
	}
	if err := block.Close(); err != nil {
		log.Warn().Str("block", ref.String()).Msg("unable to close block")
	}
}

// readBlock returns everything a block holds.
func (t *Topic) readBlock(ref common.BlockRef) ([]byte, error) {
	block, err := t.Store.Open(ref, 0)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(block)
	closeBlock(ref, block)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return content, nil
}

func (t *Topic) findLastConfirmedWrittenEntryOffset(from common.Offset) (common.Offset, bool) {
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

func (t *Topic) buildBinaryEntryRepresentation(entries common.EntriesPtr) ([]byte, int) {
	neededAllocation := 0
	for _, entry := range *entries {
		neededAllocation = neededAllocation + len(entry) + 20
	}
	var bytes = make([]byte, neededAllocation)
	start := 0
	end := 0
	entriesWritten := 0
	for _, entry := range *entries {
		byteEntry := common.CreateByteEntry(entry, t.NextOffset+common.Offset(entriesWritten))
		end = start + len(byteEntry)
		copy(bytes[start:end], byteEntry)
		start = start + len(byteEntry)
		entriesWritten = entriesWritten + 1
	}
	return bytes, entriesWritten
}

func (t *Topic) endBoundaryForReadOffset() (common.Offset, bool) {
	if t.NextOffset == 0 {
		return 0, false
	}
	return t.NextOffset, true
}

func (t *Topic) indexBlock(block common.LogBlock, byteOffset int64) (common.LogBlockPosition, error) {
	logBlock, err := t.Store.Open(t.logRef(block), byteOffset)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	indexAsBytes, newByteOffset, err := index.CreateBinaryIndexFromLog(logBlock, byteOffset, indexSparsity)
	closeBlock(t.logRef(block), logBlock)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	// an append of no pairs still creates the index block, so a reload finds it beside its
	// log block even when the block holds no offset worth indexing yet
	if _, err = t.Store.Append(t.indexRef(common.IndexBlock(block)), indexAsBytes); err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	return common.LogBlockPosition{
		Block:      block,
		ByteOffset: newByteOffset,
	}, nil
}

// findCurrentIndexLogBlockPosition returns where indexing resumes: right after the last
// indexed entry of the newest index block. It first drops index pairs that point past the
// recovered end of the log and any torn trailing bytes, so later appends stay aligned.
func (t *Topic) findCurrentIndexLogBlockPosition() (*common.LogBlockPosition, error) {
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
	position := &common.LogBlockPosition{Block: common.LogBlock(indexBlockHead)}
	if len(kept) > 0 {
		last := kept[len(kept)-1]
		entry, n, err := ibsLog.ReadEntryAt(t.Store, t.logRef(common.LogBlock(indexBlockHead)), last.ByteOffset)
		if err != nil || common.Offset(entry.Offset) != last.Offset {
			// the index does not match the log, rebuild this block's index from the start
			kept = nil
		} else {
			position.ByteOffset = last.ByteOffset + int64(n)
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

func (t *Topic) findByteOffsetInLogBlock(offset common.Offset) (int64, int, error) {
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
		return ibsLog.FindByteOffsetFromAndIncludingOffset(t.Store, t.logRef(logBlock), 0, offset)
	}
	idx, err := t.getIndexFromIndexBlock(indexBlock)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	if idx == nil {
		return ibsLog.FindByteOffsetFromAndIncludingOffset(t.Store, t.logRef(logBlock), 0, offset)
	}
	indexOffset := idx.FindNearestByteOffset(offset)
	if indexOffset.Offset > offset {
		return 0, 0, errore.NewF("found larger offset than upper bound")
	}
	if indexOffset.Offset == offset {
		return indexOffset.ByteOffset, 0, nil
	}

	return ibsLog.FindByteOffsetFromAndIncludingOffset(t.Store, t.logRef(logBlock), indexOffset.ByteOffset, offset)
}

func (t *Topic) getIndexFromIndexBlock(block common.IndexBlock) (*index.Index, error) {
	bytes, err := t.readBlock(t.indexRef(block))
	if errors.Is(err, common.ErrBlockNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return index.NewIndex(bytes), nil
}

func (t *Topic) incrementOffset(n int) {
	t.NextOffset = t.NextOffset + common.Offset(n)
}

func (t *Topic) resetHeadBlockSize() {
	t.HeadBlockSize = 0
}

func (t *Topic) addNewLogBlock() {
	t.LogBlockList = append(t.LogBlockList, common.LogBlock(t.NextOffset))
}

func (t *Topic) addNewIndexBlock(logBlock common.LogBlock) {
	t.IndexBlockList = append(t.IndexBlockList, common.IndexBlock(logBlock))
}

func (t *Topic) logBlockHead() (common.LogBlock, bool) {
	if t.logBlockIsEmpty() {
		return 0, false
	}
	return t.LogBlockList[len(t.LogBlockList)-1], true
}

func (t *Topic) indexBlockHead() (common.IndexBlock, bool) {
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

func (t *Topic) findBlocksToIndex() ([]common.LogBlock, error) {
	if t.logSize() == 0 {
		return nil, common.NoBlocksFound
	}
	logStartPos := t.indexSize() - 1
	if logStartPos < 0 {
		logStartPos = 0
	}
	return t.LogBlockList[logStartPos:], nil
}

func (t *Topic) indexBlockContaining(offset common.Offset) (common.IndexBlock, bool) {
	if t.indexSize() == 0 {
		return 0, false
	}
	for i := t.indexSize() - 1; i >= 0; i-- {
		if offset >= common.Offset(t.IndexBlockList[i]) {
			return t.IndexBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) logBlockContaining(offset common.Offset) (common.LogBlock, bool) {
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
		if offset >= common.Offset(t.LogBlockList[i]) {
			return t.LogBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) findBlockArrayIndex(block common.LogBlock) (bool, int) {
	for i, b := range t.LogBlockList {
		if b == block {
			return true, i
		}
	}
	return false, 0
}

func (t *Topic) debugLogIndexLookup(from common.Offset, byteOffset int64, scanCount int) {
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Uint64("fromOffset", uint64(from)).
			Int64("FoundByteOffset", byteOffset).
			Int("scanned", scanCount).
			Msg("read log - index scan count")
	}
}

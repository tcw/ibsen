package access

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
	ibsLog "github.com/tcw/ibsen/access/log"
	"github.com/tcw/ibsen/errore"
	"sync"
	"sync/atomic"
)

type TopicAccess interface {
	UpdateIndex() (bool, error)
	LoadOrCreate() error
	Read(params common.ReadLogParams) error
	Write(entries common.EntriesPtr) error
}

var _ TopicAccess = &Topic{}

type Topic struct {
	Afs            *afero.Afero
	RootPath       string
	TopicName      string
	indexMutex     int32
	indexWg        *sync.WaitGroup
	MaxBlockSize   int
	NextOffset     common.Offset
	HeadBlockSize  int
	LogBlockList   []common.LogBlock
	IndexBlockList []common.IndexBlock
	IndexPosition  *common.LogBlockPosition
}

func NewLogTopic(params common.TopicParams) *Topic {
	return &Topic{
		Afs:            params.Afs,
		RootPath:       params.RootPath,
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

func (t *Topic) UpdateIndex() (bool, error) {

	// Check if an index is currently running
	if !atomic.CompareAndSwapInt32(&t.indexMutex, 0, 1) {
		log.Debug().Msg("competing indices")
		return false, nil
	}
	defer atomic.CompareAndSwapInt32(&t.indexMutex, 1, 0)
	t.indexWg.Add(1)
	defer func() {
		t.indexWg.Done()
	}()

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
	created, err := ibsLog.CreateTopicDirectory(t.Afs, t.RootPath, t.TopicName)
	if created {
		return nil
	}
	// Load log and index blocks from file
	logBlocks, indexBlocks, err := ibsLog.LoadTopicBlocks(t.Afs, t.RootPath, t.TopicName)
	if err != nil {
		return err
	}
	if len(logBlocks) == 0 {
		return common.NoBlocksFound
	}
	t.IndexBlockList = indexBlocks
	t.LogBlockList = logBlocks

	// Find position of last entry write to log
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errore.New("Topic " + t.TopicName + " has no block head")
	}
	offset, byteSize, err := t.findCurrentLogPosition(err, head)
	if err != nil {
		return errore.Wrap(err)
	}
	t.NextOffset = offset + 1
	t.HeadBlockSize = int(byteSize)

	// Find position of last entry write to index
	position, _, err := t.findCurrentIndexLogBlockPosition()
	if err != nil {
		return errore.Wrap(err)
	}
	t.IndexPosition = position
	t.debugLogLoadResult(logBlocks, indexBlocks)
	return nil
}

// ReadLog
// Reads a log from and including the ReadLogParams.From offset until end of log.
func (t *Topic) Read(params common.ReadLogParams) error {
	// ensures reader will not read partially written log entries from file
	endOffset, exists := t.findLastConfirmedWrittenEntryOffset(params.From)
	if !exists {
		return common.NoEntriesFound
	}
	block, found := t.logBlockContaining(params.From)
	if !found {
		return errore.New("offset out of bounds, this should never happen!")
	}

	// find byte offset in file to set seek point to
	byteOffset, scanCount, err := t.findByteOffsetInLogBlockFile(params.From)
	if err == ibsLog.NoByteOffsetFound {
		return common.NoEntriesFound
	}
	if err != nil {
		return errore.Wrap(err)
	}
	t.debugLogIndexLookup(params.From, byteOffset, scanCount)

	// find log file that contains offset
	fileName, err := t.logBlockFileName(block)
	if err != nil {
		return errore.Wrap(err)
	}
	file, err := common.OpenFileForRead(t.Afs, fileName)
	if err != nil {
		return errore.Wrap(err)
	}

	// read log file from byte offset position (with seek)
	_, err = ibsLog.ReadFile(ibsLog.ReadFileParams{
		File:            file,
		LogChan:         params.LogChan,
		Wg:              params.Wg,
		BatchSize:       params.BatchSize,
		StartByteOffset: byteOffset,
		EndOffset:       endOffset,
	})
	if err != nil {
		closeFile(file)
		return errore.Wrap(err)
	}
	closeFile(file)

	// read remaining log files
	wasFound, i := t.findBlockArrayIndex(block)
	if wasFound {
		if t.logSize()-1 == i {
			return nil
		}
		for _, b := range t.LogBlockList[i+1:] {
			fileName, err = t.logBlockFileName(b)
			file, err = common.OpenFileForRead(t.Afs, fileName)
			if errors.Is(err, common.FileNotFound) {
				closeFile(file)
				break
			}
			if err != nil {
				closeFile(file)
				return errore.Wrap(err)
			}
			endOffset, _ = t.endBoundaryForReadOffset()
			_, err = ibsLog.ReadFile(ibsLog.ReadFileParams{
				File:            file,
				LogChan:         params.LogChan,
				Wg:              params.Wg,
				BatchSize:       params.BatchSize,
				StartByteOffset: 0,
				EndOffset:       endOffset,
			})
			if err != nil {
				closeFile(file)
				return errore.Wrap(err)
			}
			closeFile(file)
		}
	}
	return nil
}

func (t *Topic) Write(entries common.EntriesPtr) error {

	// if topic is empty create the first log block
	if t.logBlockIsEmpty() {
		t.addNewLogBlock()
	}
	// if block has excited is max size create a new block
	if t.HeadBlockSize > t.MaxBlockSize {
		t.addNewLogBlock()
		t.resetHeadBlockSize()
	}
	// create a byte representation of entries and write to disk
	bytes, offsets := t.buildBinaryEntryRepresentation(entries)
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errors.New("Topic " + t.TopicName + " has no block head")
	}
	blockFileName, err := t.logBlockFileName(head)

	file, err := common.OpenFileForWrite(t.Afs, blockFileName)
	if err != nil {
		ioErr := file.Close()
		if ioErr != nil {
			return errore.WrapError(ioErr, err)
		}
		return err
	}

	n, err := file.Write(bytes)
	if err != nil {
		ioErr := file.Close()
		if ioErr != nil {
			return errore.WrapError(ioErr, err)
		}
		return err
	}

	// update internal log state
	t.incrementOffset(offsets)
	t.incrementHeadBlockSize(n)

	// update index async if no index is running
	go func() {
		wasExecuted, err := t.UpdateIndex()
		if err != nil {
			log.Warn().Err(err)
		}
		log.Trace().Msg(fmt.Sprintf("index update executed: %t", wasExecuted))
	}()
	ioErr := file.Close()
	if ioErr != nil {
		return ioErr
	}
	return nil
}

func (t *Topic) findCurrentLogPosition(err error, head common.LogBlock) (common.Offset, int64, error) {
	blockFileName, err := t.logBlockFileName(head)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	offset, byteSize, err := ibsLog.BlockInfo(t.Afs, blockFileName)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	return offset, byteSize, nil
}

func (t *Topic) debugLogLoadResult(logBlocks []common.LogBlock, indexBlocks []common.IndexBlock) {
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

func closeFile(file afero.File) {
	if file == nil {
		return
	}
	err := file.Close()
	if err != nil {
		log.Warn().Str("fileName", file.Name()).Msg("unable to close file")
	}
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

func (t *Topic) logBlockFileName(block common.LogBlock) (string, error) {
	if t.logBlockIsEmpty() {
		return "", common.NoBlocksFound
	}
	return t.RootPath + common.Sep + t.TopicName + common.Sep + fmt.Sprintf("%020d.log", block), nil
}

func (t *Topic) indexBlockFileName(block common.IndexBlock) (string, error) {
	if t.logBlockIsEmpty() {
		return "", common.NoBlocksFound
	}
	return t.RootPath + common.Sep + t.TopicName + common.Sep + fmt.Sprintf("%020d.idx", block), nil
}

func (t *Topic) indexBlock(block common.LogBlock, byteOffset int64) (common.LogBlockPosition, error) {
	logBlockFilename, err := t.logBlockFileName(block)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	indexAsBytes, newByteOffset, err := index.CreateBinaryIndexFromLogFile(t.Afs, logBlockFilename, byteOffset, 10)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	indexBlockFilename, err := t.indexBlockFileName(common.IndexBlock(block))
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	file, err := common.OpenFileForWrite(t.Afs, indexBlockFilename)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	_, err = file.Write(indexAsBytes)
	if err != nil {
		return common.LogBlockPosition{}, errore.Wrap(err)
	}
	return common.LogBlockPosition{
		Block:      block,
		ByteOffset: newByteOffset,
	}, nil
}

func (t *Topic) findCurrentIndexLogBlockPosition() (*common.LogBlockPosition, bool, error) {
	indexBlockHead, hasBlock := t.indexBlockHead()
	if !hasBlock {
		return nil, false, nil
	}
	indexBlockFileName, err := t.indexBlockFileName(indexBlockHead)
	if err != nil {
		return nil, false, errore.Wrap(err)
	}
	byteIndex, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return nil, false, errore.Wrap(err)
	}
	index := index.NewIndex(byteIndex)
	head := index.Head()
	return &common.LogBlockPosition{
		Block:      common.LogBlock(indexBlockHead),
		ByteOffset: head.ByteOffset,
	}, true, nil
}

func (t *Topic) findByteOffsetInLogBlockFile(offset common.Offset) (int64, int, error) {
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
	logBlockFileName, err := t.logBlockFileName(logBlock)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	if !foundIndexBlock {
		return ibsLog.FindByteOffsetFromAndIncludingOffset(t.Afs, logBlockFileName, 0, offset)
	}
	idx, err := t.getIndexFromIndexBlock(indexBlock)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	indexOffset := idx.FindNearestByteOffset(offset)
	if indexOffset.Offset > offset {
		return 0, 0, errore.NewF("found larger offset than upper bound")
	}
	if indexOffset.Offset == offset {
		return indexOffset.ByteOffset, 0, nil
	}

	return ibsLog.FindByteOffsetFromAndIncludingOffset(t.Afs, logBlockFileName, indexOffset.ByteOffset, offset)
}

func (t *Topic) getIndexFromIndexBlock(block common.IndexBlock) (*index.Index, error) {
	indexBlockFileName, err := t.indexBlockFileName(block)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	exists, err := t.Afs.Exists(indexBlockFileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	bytes, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	idx := index.NewIndex(bytes)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return idx, nil
}

func (t *Topic) incrementOffset(n int) {
	t.NextOffset = t.NextOffset + common.Offset(n)
}

func (t *Topic) incrementHeadBlockSize(n int) {
	t.HeadBlockSize = t.HeadBlockSize + n
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

func (t *Topic) getLogBlock(index int) common.LogBlock {
	return t.LogBlockList[index]
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

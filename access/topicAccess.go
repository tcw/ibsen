package access

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"sync"
	"sync/atomic"
)

type Offset uint64
type LogBlock uint64
type IndexBlock uint64
type BlockIndex uint32

type LogBlockPosition struct {
	Block      LogBlock
	ByteOffset int64
}

type TopicAccess interface {
	UpdateIndex() (bool, error)
	Load() error
	ReadLog(params ReadLogParams) error
	Write(entries EntriesPtr) error
}

var _ TopicAccess = &Topic{}

var BlockNotFound = errors.New("block not found")

type EntriesPtr *[][]byte

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type Topic struct {
	Afs            *afero.Afero
	RootPath       string
	TopicName      string
	indexMutex     int32
	indexWg        *sync.WaitGroup
	MaxBlockSize   int
	NextOffset     Offset
	HeadBlockSize  int
	LogBlockList   []LogBlock
	IndexBlockList []IndexBlock
	IndexPosition  *LogBlockPosition
}

type TopicParams struct {
	Afs          *afero.Afero
	RootPath     string
	TopicName    string
	MaxBlockSize int
}

func NewLogTopic(params TopicParams) *Topic {
	return &Topic{
		Afs:            params.Afs,
		RootPath:       params.RootPath,
		TopicName:      params.TopicName,
		indexWg:        &sync.WaitGroup{},
		NextOffset:     0,
		HeadBlockSize:  0,
		MaxBlockSize:   params.MaxBlockSize,
		LogBlockList:   []LogBlock{},
		IndexBlockList: []IndexBlock{},
		IndexPosition:  nil,
	}
}

func (t *Topic) UpdateIndex() (bool, error) {
	if !atomic.CompareAndSwapInt32(&t.indexMutex, 0, 1) {
		log.Debug().Msg("competing indices")
		return false, nil
	}
	defer atomic.CompareAndSwapInt32(&t.indexMutex, 1, 0)

	t.indexWg.Add(1)
	defer func() {
		t.indexWg.Done()
	}()

	notIndexed, err := t.findLogBlocksNotIndexed()
	if err != nil {
		return true, errore.Wrap(err)
	}
	for _, block := range notIndexed {
		if t.IndexPosition == nil {
			pos, err := t.indexBlock(block, 0)
			if err != nil {
				return true, errore.Wrap(err)
			}
			t.addNewIndexBlock(block)
			t.IndexPosition = &pos
		} else {
			position := t.IndexPosition
			if position.Block == block {
				pos, err := t.indexBlock(block, position.ByteOffset)
				if err != nil {
					return true, errore.Wrap(err)
				}
				t.IndexPosition = &pos
			} else {
				pos, err := t.indexBlock(block, 0)
				if err != nil {
					return true, errore.Wrap(err)
				}
				t.addNewIndexBlock(block)
				t.IndexPosition = &pos
			}
		}
	}
	return true, nil
}

func (t *Topic) Load() error {
	logBlocks, indexBlocks, err := LoadTopicBlocks(t.Afs, t.RootPath, t.TopicName)

	if err != nil {
		return err
	}
	t.IndexBlockList = indexBlocks
	t.LogBlockList = logBlocks
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errors.New("Topic " + t.TopicName + " has no block head")
	}
	blockFileName, err := t.logBlockFileName(head)
	if err != nil {
		return errore.Wrap(err)
	}
	offset, byteSize, err := BlockInfo(t.Afs, blockFileName)
	if err != nil {
		return errore.Wrap(err)
	}
	t.NextOffset = offset + 1
	t.HeadBlockSize = int(byteSize)
	position, _, err := t.findLastIndexLogBlockPosition()
	if err != nil {
		return errore.Wrap(err)
	}
	t.IndexPosition = position
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Int("logBlocks", len(logBlocks)).
			Int("indexBlocks", len(indexBlocks)).
			Int("nextOffset", int(t.NextOffset)).
			Int("headBlockSize", t.HeadBlockSize).
			Msg("loaded topic")
	}
	return nil
}

var NoEntriesFound = errors.New("no entries found")

type ReadLogParams struct {
	LogChan   chan *[]LogEntry
	Wg        *sync.WaitGroup
	From      Offset
	BatchSize uint32
}

// ReadLog
// Reads a log from and including the ReadLogParams.From offset until end of log.
func (t *Topic) ReadLog(params ReadLogParams) error {
	endOffset, exists := t.findEndOffset(params.From)
	if !exists {
		return NoEntriesFound
	}
	block, found := t.logBlockContaining(params.From)
	if !found {
		return errors.New("offset out of bounds")
	}
	byteOffset, scanCount, err := t.findByteOffsetInLogBlockFile(params.From)
	if err == NoByteOffsetFound {
		return NoEntriesFound
	}
	if err != nil {
		return errore.Wrap(err)
	}
	t.debugLogIndexLookup(params.From, byteOffset, scanCount)
	fileName, err := t.logBlockFileName(block)
	if err != nil {
		return errore.Wrap(err)
	}
	file, err := OpenFileForRead(t.Afs, fileName)
	if err != nil {
		return errore.Wrap(err)
	}
	_, err = ReadFile(ReadFileParams{
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
	wasFound, i := t.findBlockArrayIndex(block)
	if wasFound {
		if t.logSize()-1 == i {
			return nil
		}
		for _, b := range t.LogBlockList[i+1:] {
			fileName, err = t.logBlockFileName(b)
			file, err = OpenFileForRead(t.Afs, fileName)
			if errors.Is(err, FileNotFound) {
				closeFile(file)
				break
			}
			if err != nil {
				closeFile(file)
				return errore.Wrap(err)
			}
			endOffset, _ = t.endBoundaryForReadOffset()
			_, err = ReadFile(ReadFileParams{
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

func closeFile(file afero.File) {
	if file == nil {
		return
	}
	err := file.Close()
	if err != nil {
		log.Warn().Str("fileName", file.Name()).Msg("unable to close file")
	}
}

func (t *Topic) findEndOffset(from Offset) (Offset, bool) {
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

func (t *Topic) debugLogIndexLookup(from Offset, byteOffset int64, scanCount int) {
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Uint64("fromOffset", uint64(from)).
			Int64("FoundByteOffset", byteOffset).
			Int("scanned", scanCount).
			Msg("read log - index scan count")
	}
}

func (t *Topic) Write(entries EntriesPtr) error {
	if t.logBlockIsEmpty() {
		t.addNewLogBlock()
	}
	if t.HeadBlockSize > t.MaxBlockSize {
		t.addNewLogBlock()
		t.resetHeadBlockSize()
	}

	neededAllocation := 0
	for _, entry := range *entries {
		neededAllocation = neededAllocation + len(entry) + 20
	}
	var bytes = make([]byte, neededAllocation)
	start := 0
	end := 0
	entriesWritten := 0
	for _, entry := range *entries {
		byteEntry := CreateByteEntry(entry, t.NextOffset+Offset(entriesWritten))
		end = start + len(byteEntry)
		copy(bytes[start:end], byteEntry)
		start = start + len(byteEntry)
		entriesWritten = entriesWritten + 1
	}
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errors.New("Topic " + t.TopicName + " has no block head")
	}
	blockFileName, err := t.logBlockFileName(head)
	file, err := openFileForWrite(t.Afs, blockFileName)

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
	t.incrementOffset(entriesWritten)
	t.incrementHeadBlockSize(n)
	if err != nil {
		ioErr := file.Close()
		if ioErr != nil {
			return errore.WrapError(ioErr, err)
		}
		return err
	}
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

func (t *Topic) ToString() string {
	list := t.LogBlockList
	blocklist := ""
	for i, val := range list {
		blocklist = blocklist + fmt.Sprintf("%d -> %d\n", i, val)
	}
	return blocklist
}

func (t *Topic) endBoundaryForReadOffset() (Offset, bool) {
	if t.NextOffset == 0 {
		return 0, false
	}
	return t.NextOffset, true
}

func (t *Topic) logBlockFileName(block LogBlock) (string, error) {
	if t.logBlockIsEmpty() {
		return "", BlockNotFound
	}
	return t.RootPath + Sep + t.TopicName + Sep + fmt.Sprintf("%020d.log", block), nil
}

func (t *Topic) indexBlockFileName(block IndexBlock) (string, error) {
	if t.logBlockIsEmpty() {
		return "", BlockNotFound
	}
	return t.RootPath + Sep + t.TopicName + Sep + fmt.Sprintf("%020d.idx", block), nil
}

func (t *Topic) findLastIndexLogBlockPosition() (*LogBlockPosition, bool, error) {
	indexBlockHead, hasHead := t.indexBlockHead()
	if !hasHead {
		return nil, false, nil
	}
	indexBlockFileName, err := t.indexBlockFileName(indexBlockHead)
	if err != nil {
		return nil, false, errore.Wrap(err)
	}
	indexAsBytes, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return nil, false, errore.Wrap(err)
	}
	index, err := MarshallIndex(indexAsBytes)
	indexOffsetHead := index.Head()
	return &LogBlockPosition{
		Block:      LogBlock(indexBlockHead),
		ByteOffset: indexOffsetHead.ByteOffset,
	}, true, nil
}

func (t *Topic) indexBlock(block LogBlock, byteOffset int64) (LogBlockPosition, error) {
	logBlockFilename, err := t.logBlockFileName(block)
	if err != nil {
		return LogBlockPosition{}, errore.Wrap(err)
	}
	indexAsBytes, newByteOffset, err := CreateIndex(t.Afs, logBlockFilename, byteOffset, 10)
	if err != nil {
		return LogBlockPosition{}, errore.Wrap(err)
	}
	indexBlockFilename, err := t.indexBlockFileName(IndexBlock(block))
	if err != nil {
		return LogBlockPosition{}, errore.Wrap(err)
	}
	file, err := openFileForWrite(t.Afs, indexBlockFilename)
	if err != nil {
		return LogBlockPosition{}, errore.Wrap(err)
	}
	_, err = file.Write(indexAsBytes)
	if err != nil {
		return LogBlockPosition{}, errore.Wrap(err)
	}
	return LogBlockPosition{
		Block:      block,
		ByteOffset: newByteOffset,
	}, nil
}

func (t *Topic) findCurrentIndexLogBlockPosition() (LogBlockPosition, bool, error) {
	indexBlockHead, hasBlock := t.indexBlockHead()
	if !hasBlock {
		return LogBlockPosition{}, false, nil
	}
	indexBlockFileName, err := t.indexBlockFileName(indexBlockHead)
	if err != nil {
		return LogBlockPosition{}, false, errore.Wrap(err)
	}
	byteIndex, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return LogBlockPosition{}, false, errore.Wrap(err)
	}
	index, err := MarshallIndex(byteIndex)
	if err != nil {
		return LogBlockPosition{}, false, errore.Wrap(err)
	}
	head := index.Head()
	return LogBlockPosition{
		Block:      LogBlock(indexBlockHead),
		ByteOffset: head.ByteOffset,
	}, true, nil
}

type OffsetPosition struct {
	logBlock       LogBlock
	byteOffset     int64
	entriesScanned int
	indexEntryUsed IndexOffset
}

func (t *Topic) findByteOffsetInLogBlockFile(offset Offset) (int64, int, error) {
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
		return FindByteOffsetFromAndIncludingOffset(t.Afs, logBlockFileName, 0, offset)
	}
	index, err := t.getIndexFromIndexBlock(indexBlock)
	if err != nil {
		return 0, 0, errore.Wrap(err)
	}
	indexOffset := index.findNearestByteOffset(offset)
	if indexOffset.Offset > offset {
		return 0, 0, errore.NewF("found larger offset than upper bound")
	}
	if indexOffset.Offset == offset {
		return indexOffset.ByteOffset, 0, nil
	}

	return FindByteOffsetFromAndIncludingOffset(t.Afs, logBlockFileName, indexOffset.ByteOffset, offset)
}

func (t *Topic) getIndexFromIndexBlock(block IndexBlock) (Index, error) {
	indexBlockFileName, err := t.indexBlockFileName(block)
	if err != nil {
		return Index{}, errore.Wrap(err)
	}
	exists, err := t.Afs.Exists(indexBlockFileName)
	if err != nil {
		return Index{}, err
	}
	if !exists {
		return Index{}, nil
	}
	bytes, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return Index{}, errore.Wrap(err)
	}
	index, err := MarshallIndex(bytes)
	if err != nil {
		return Index{}, errore.Wrap(err)
	}
	return index, nil
}

func (t *Topic) incrementOffset(n int) {
	t.NextOffset = t.NextOffset + Offset(n)
}

func (t *Topic) incrementHeadBlockSize(n int) {
	t.HeadBlockSize = t.HeadBlockSize + n
}

func (t *Topic) resetHeadBlockSize() {
	t.HeadBlockSize = 0
}

func (t *Topic) addNewLogBlock() {
	t.LogBlockList = append(t.LogBlockList, LogBlock(t.NextOffset))
}

func (t *Topic) addNewIndexBlock(logBlock LogBlock) {
	t.IndexBlockList = append(t.IndexBlockList, IndexBlock(logBlock))
}

func (t *Topic) logBlockHead() (LogBlock, bool) {
	if t.logBlockIsEmpty() {
		return 0, false
	}
	return t.LogBlockList[len(t.LogBlockList)-1], true
}

func (t *Topic) indexBlockHead() (IndexBlock, bool) {
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

func (t *Topic) getLogBlock(index int) LogBlock {
	return t.LogBlockList[index]
}

func (t *Topic) logSize() int {
	return len(t.LogBlockList)
}

func (t *Topic) indexSize() int {
	return len(t.IndexBlockList)
}

func (t *Topic) findLogBlocksNotIndexed() ([]LogBlock, error) {
	if t.logSize() == 0 {
		return nil, BlockNotFound
	}
	logStartPos := t.indexSize()
	return t.LogBlockList[logStartPos:], nil
}

func (t *Topic) tail() ([]LogBlock, error) {
	if t.logSize() < 2 {
		return []LogBlock{}, BlockNotFound
	}
	return t.LogBlockList[:t.logSize()-2], nil
}

func (t *Topic) indexBlockContaining(offset Offset) (IndexBlock, bool) {
	if t.indexSize() == 0 {
		return 0, false
	}
	for i := t.indexSize() - 1; i >= 0; i-- {
		if offset >= Offset(t.IndexBlockList[i]) {
			return t.IndexBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) logBlockContaining(offset Offset) (LogBlock, bool) {
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
		if offset >= Offset(t.LogBlockList[i]) {
			return t.LogBlockList[i], true
		}
	}
	return 0, false
}

func (t *Topic) findBlockArrayIndex(block LogBlock) (bool, int) {
	for i, b := range t.LogBlockList {
		if b == block {
			return true, i
		}
	}
	return false, 0
}

func (t *Topic) getBlocksIncludingAndAfter(offset Offset) ([]LogBlock, error) {
	if t.logSize() <= 0 {
		return []LogBlock{}, BlockNotFound
	}
	if t.logSize() == 1 {
		return t.LogBlockList[0:], nil
	}
	if offset < Offset(t.getLogBlock(0)) {
		return t.LogBlockList[0:], nil
	}
	for i := t.logSize() - 1; i >= 0; i-- {
		if offset >= Offset(t.LogBlockList[i]) {
			return t.LogBlockList[i:], nil
		}
	}
	return []LogBlock{}, BlockNotFound
}

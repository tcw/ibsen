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
	UpdateIndex() error
	Load() error
	IsLoaded() bool
	Read(logChan chan *[]LogEntry, wg *sync.WaitGroup, from Offset, batchSize uint32) error
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
	writeLock      sync.Mutex
	updateLock     sync.Mutex
	indexMutex     int32
	MaxBlockSize   int
	NextOffset     Offset
	HeadBlockSize  int
	LogBlockList   []LogBlock
	IndexBlockList []IndexBlock
	IndexPosition  *LogBlockPosition
	Loaded         bool
}

func NewLogTopic(afs *afero.Afero, rootPath string, topicName string, maxBlockSize int, loaded bool) *Topic {
	return &Topic{
		Afs:            afs,
		RootPath:       rootPath,
		TopicName:      topicName,
		writeLock:      sync.Mutex{},
		updateLock:     sync.Mutex{},
		NextOffset:     0,
		HeadBlockSize:  0,
		MaxBlockSize:   maxBlockSize,
		LogBlockList:   []LogBlock{},
		IndexBlockList: []IndexBlock{},
		IndexPosition:  nil,
		Loaded:         loaded,
	}
}

func (t *Topic) UpdateIndex() error {
	if !atomic.CompareAndSwapInt32(&t.indexMutex, 0, 1) {
		return nil
	}
	defer atomic.CompareAndSwapInt32(&t.indexMutex, 1, 0)

	t.updateLock.Lock()
	defer t.updateLock.Unlock()

	notIndexed, err := t.findLogBlocksNotIndexed()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for _, block := range notIndexed {
		if t.IndexPosition == nil {
			pos, err := t.indexBlock(block, 0)
			if err != nil {
				return errore.WrapWithContext(err)
			}
			t.IndexPosition = &pos
		} else {
			position := t.IndexPosition
			if position.Block == block {
				pos, err := t.indexBlock(block, position.ByteOffset)
				if err != nil {
					return errore.WrapWithContext(err)
				}
				t.IndexPosition = &pos
			} else {
				pos, err := t.indexBlock(block, 0)
				if err != nil {
					return errore.WrapWithContext(err)
				}
				t.IndexPosition = &pos
			}
		}
	}
	return nil
}

func (t *Topic) IsLoaded() bool {
	return t.Loaded
}

func (t *Topic) Load() error {
	t.updateLock.Lock()
	defer t.updateLock.Unlock()
	if t.Loaded {
		return nil
	}
	logBlocks, indexBlocks, err := LoadTopicBlocks(t.Afs, t.RootPath, t.TopicName)

	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.IndexBlockList = indexBlocks
	t.LogBlockList = logBlocks
	head, hasBlockHead := t.logBlockHead()
	if !hasBlockHead {
		return errore.WrapWithContext(errors.New("Topic " + t.TopicName + " has no block head"))
	}
	blockFileName, err := t.logBlockFileName(head)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	offset, byteSize, err := FindBlockInfo(t.Afs, blockFileName)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.NextOffset = offset + 1
	t.HeadBlockSize = int(byteSize)
	position, _, err := t.findLastIndexLogBlockPosition()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.IndexPosition = &position
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Int("logBlocks", len(logBlocks)).
			Int("indexBlocks", len(indexBlocks)).
			Int("nextOffset", int(t.NextOffset)).
			Int("headBlockSize", t.HeadBlockSize).
			Msg("loaded topic")
	}
	t.Loaded = true
	return nil
}

func (t *Topic) Read(logChan chan *[]LogEntry, wg *sync.WaitGroup, from Offset, batchSize uint32) error {
	if t.logBlockIsEmpty() {
		return nil
	}
	block, found := t.logBlockContaining(from)
	if !found {
		return errors.New("offset out of bounds")
	}
	byteOffset, scanCount, err := t.findByteOffsetInIndex(from)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if e := log.Debug(); e.Enabled() {
		e.Str("topic", t.TopicName).
			Uint64("offset", uint64(from)).
			Int64("byteOffset", byteOffset).
			Int("scanned", scanCount).
			Msg("index scan count")
	}
	fileName, err := t.logBlockFileName(block)
	endOffset, hasEnd := t.endBoundaryForReadOffset()
	if !hasEnd {
		return nil
	}
	file, err := OpenFileForRead(t.Afs, fileName)
	err = ReadFile(file, logChan, wg, batchSize, byteOffset, endOffset)
	wasFound, i := t.findBlockArrayIndex(block)
	if wasFound {
		if t.logSize()-1 == i {
			return nil
		}
		for _, b := range t.LogBlockList[i+1:] {
			fileName, err = t.logBlockFileName(b)
			file, err = OpenFileForRead(t.Afs, fileName)
			endOffset, _ = t.endBoundaryForReadOffset()
			err = ReadFile(file, logChan, wg, batchSize, 0, endOffset)
		}
	}
	return nil
}

func (t *Topic) Write(entries EntriesPtr) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

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
		return errore.WrapWithContext(errors.New("Topic " + t.TopicName + " has no block head"))
	}
	blockFileName, err := t.logBlockFileName(head)
	file, err := openFileForWrite(t.Afs, blockFileName)
	defer file.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	n, err := file.Write(bytes)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.incrementOffset(entriesWritten)
	t.incrementHeadBlockSize(n)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	go func() {
		err := t.UpdateIndex()
		if err != nil {
			log.Warn().Err(err)
		}
	}()
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
	return t.NextOffset - 1, true
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

func (t *Topic) findLastIndexLogBlockPosition() (LogBlockPosition, bool, error) {
	indexBlockHead, hasHead := t.indexBlockHead()
	if !hasHead {
		return LogBlockPosition{}, false, nil
	}
	indexBlockFileName, err := t.indexBlockFileName(indexBlockHead)
	if err != nil {
		return LogBlockPosition{}, false, errore.WrapWithContext(err)
	}
	indexAsBytes, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return LogBlockPosition{}, false, errore.WrapWithContext(err)
	}
	index, err := MarshallIndex(indexAsBytes)
	indexOffsetHead := index.Head()
	return LogBlockPosition{
		Block:      LogBlock(indexBlockHead),
		ByteOffset: indexOffsetHead.ByteOffset,
	}, true, nil
}

func (t *Topic) indexBlock(block LogBlock, byteOffset int64) (LogBlockPosition, error) {
	logBlockFilename, err := t.logBlockFileName(block)
	if err != nil {
		return LogBlockPosition{}, errore.WrapWithContext(err)
	}
	indexAsBytes, newByteOffset, err := CreateIndex(t.Afs, logBlockFilename, byteOffset, 10)
	if err != nil {
		return LogBlockPosition{}, errore.WrapWithContext(err)
	}
	indexBlockFilename, err := t.indexBlockFileName(IndexBlock(block))
	if err != nil {
		return LogBlockPosition{}, errore.WrapWithContext(err)
	}
	file, err := openFileForWrite(t.Afs, indexBlockFilename)
	if err != nil {
		return LogBlockPosition{}, errore.WrapWithContext(err)
	}
	_, err = file.Write(indexAsBytes)
	if err != nil {
		return LogBlockPosition{}, errore.WrapWithContext(err)
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
		return LogBlockPosition{}, false, errore.WrapWithContext(err)
	}
	byteIndex, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return LogBlockPosition{}, false, errore.WrapWithContext(err)
	}
	index, err := MarshallIndex(byteIndex)
	if err != nil {
		return LogBlockPosition{}, false, errore.WrapWithContext(err)
	}
	head := index.Head()
	return LogBlockPosition{
		Block:      LogBlock(indexBlockHead),
		ByteOffset: head.ByteOffset,
	}, true, nil
}

func (t *Topic) findByteOffsetInIndex(offset Offset) (int64, int, error) {
	indexBlock, foundIndexBlock := t.indexBlockContaining(offset)
	if offset >= t.NextOffset {
		return 0, 0, errors.New("offset out of bounds")
	}
	logBlock, logBlockFound := t.logBlockContaining(offset)
	if !logBlockFound {
		return 0, 0, errors.New("no log block containing offset found")
	}
	logBlockFileName, err := t.logBlockFileName(logBlock)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	if !foundIndexBlock {
		return FindByteOffsetFromOffset(t.Afs, logBlockFileName, 0, offset)
	}
	indexBlockFileName, err := t.indexBlockFileName(indexBlock)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	bytes, err := t.Afs.ReadFile(indexBlockFileName)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	index, err := MarshallIndex(bytes)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	indexOffset := index.findNearestByteOffset(offset)
	if indexOffset.Offset > offset {
		return 0, 0, errore.NewWithContext("found larger offset than upper bound")
	}
	if indexOffset.Offset == offset {
		return indexOffset.ByteOffset, 0, nil
	}

	return FindByteOffsetFromOffset(t.Afs, logBlockFileName, indexOffset.ByteOffset, offset)
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

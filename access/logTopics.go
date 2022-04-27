package access

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"os"
	"sort"
	"sync"
)

const Sep = string(os.PathSeparator)

type Offset uint64
type Block uint64
type BlockIndex uint32


var BlockNotFound = errors.New("block not found")

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type Topic struct {
	Afs                    *afero.Afero
	RootPath               string
	TopicName              string
	MaxBlockSize   int
	NextOffset     Offset
	HeadBlockSize  int
	LogBlockList   []Block
	IndexBlockList []Block
	WorkingIndex   Index
	WorkingIndexLogPointer int
}

type Entries *[][]byte

func newLogTopic(afs *afero.Afero, rootPath string, topicName string, maxBlockSize int) Topic {
	return Topic{
		Afs:                    afs,
		RootPath:               rootPath,
		TopicName:              topicName,
		NextOffset:             0,
		HeadBlockSize:          0,
		MaxBlockSize:           maxBlockSize,
		LogBlockList:           []Block{},
		IndexBlockList:         []Block{},
		WorkingIndex:           Index{},
		WorkingIndexLogPointer: 0,
	}
}

func (t Topic) UpdateIndex() {

}

func (t *Topic) Load() {

}

func (t Topic) Read(logChan chan *[]LogEntry, wg *sync.WaitGroup, from Offset, batchSize uint32) error {
	if t.logBlockIsEmpty() {
		return
	}
	logBlock := t.LogBlockContaining(from)
	byteOffset, err := t.findByteOffsetInIndex(from)
	if err != nil{
		return errore.WrapWithContext(err)
	}
	OpenFileForRead(t.Afs,t.)
	ReadFile(t.Afs,logChan,wg,batchSize,byteOffset)
}

func (t *Topic) Write(entries Entries) error {
	if t.logBlockIsEmpty() {
		t.addNewLogBlock()
	}
	if t.logSize() > t.MaxBlockSize {
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
	for _, entry := range *entries {
		byteEntry := createByteEntry(entry, t.NextOffset)
		end = start + len(byteEntry)
		copy(bytes[start:end], byteEntry)
		start = start + len(byteEntry)
		t.incrementOffset(1)
	}
	head, err := t.head()
	if err != nil {
		return errore.WrapWithContext(err)
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
	t.incrementHeadBlockSize(n)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (t *Topic) logBlockFileName(block Block) (string, error) {
	if t.logBlockIsEmpty() {
		return "", BlockNotFound
	}
	return t.RootPath + Sep + t.TopicName + Sep + fmt.Sprintf("%020d.log", block), nil
}

func (t *Topic) indexBlockFileName(block Block) (string, error) {
	if t.logBlockIsEmpty() {
		return "", BlockNotFound
	}
	return t.RootPath + Sep + t.TopicName + Sep + fmt.Sprintf("%020d.idx", block), nil
}

func (t Topic) findByteOffsetInIndex(offset Offset) (int64, error) {
	indexBlock, err := t.IndexBlockContaining(offset)
	if err == BlockNotFound {
		// scan log for byteoffset
	}
	fileName, err := t.indexBlockFileName(indexBlock)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	file, err := OpenFileForRead(t.Afs, fileName)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	defer file.Close()

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
	t.LogBlockList = append(t.LogBlockList, Block(t.NextOffset))
}

func (t Topic) head() (Block, error) {
	if t.logBlockIsEmpty() {
		return 0, BlockNotFound
	}
	return t.LogBlockList[len(t.LogBlockList)-1], nil
}

func (t Topic) logBlockIsEmpty() bool {
	return len(t.LogBlockList) == 0
}

func (t Topic) getLogBlock(index int) Block {
	return t.LogBlockList[index]
}

func (t Topic) logSize() int {
	return len(t.LogBlockList)
}

func (t Topic) indexSize() int {
	return len(t.IndexBlockList)
}

func (t Topic) ToString() string {
	list := t.LogBlockList
	blocklist := ""
	for i, val := range list {
		blocklist = blocklist + fmt.Sprintf("%d -> %d\n", i, val)
	}
	return blocklist
}

func (t *Topic) sort() {
	if t.logBlockIsEmpty() {
		return
	}
	sort.Slice(t.LogBlockList, func(i, j int) bool { return t.LogBlockList[i] < t.LogBlockList[j] })
}

func (t Topic) findLogBlocksNotIndexed() ([]Block, error) {
	if t.logSize() == 0 {
		return nil, BlockNotFound
	}
	logStartPos := t.indexSize()
	logEndPos := t.logSize() - 1

	return t.LogBlockList[logStartPos:logEndPos], nil
}

func (t Topic) Tail() ([]Block, error) {
	if t.logSize() < 2 {
		return []Block{}, BlockNotFound
	}
	return t.LogBlockList[:t.logSize()-2], nil
}

func (t Topic) IndexBlockContaining(offset Offset) (Block, error) {
	if t.indexSize() == 0 {
		return 0, BlockNotFound
	}
	for i := t.indexSize() - 1; i >= 0; i-- {
		if offset >= Offset(t.IndexBlockList[i]) {
			return t.LogBlockList[i], nil
		}
	}
	return 0, BlockNotFound
}

func (t Topic) LogBlockContaining(offset Offset) (Block, error) {
	if t.logSize() == 0 {
		return 0, BlockNotFound
	}
	if t.logSize() == 1 {
		return t.LogBlockList[0], nil
	}
	for i := t.logSize() - 1; i >= 0; i-- {
		if offset >= Offset(t.LogBlockList[i]) {
			return t.LogBlockList[i], nil
		}
	}
	return 0, BlockNotFound
}

func (t Topic) GetBlocksIncludingAndAfter(offset Offset) ([]Block, error) {
	if t.logSize() <= 0 {
		return []Block{}, BlockNotFound
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
	return []Block{}, BlockNotFound
}





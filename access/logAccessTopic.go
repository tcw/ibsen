package access

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"sort"
	"sync"
)

var BlockNotFound = errors.New("block not found")

type TopicHandler struct {
	Afs                *afero.Afero
	logMutex           *sync.Mutex
	indexMutex         *sync.Mutex
	Topic              Topic
	RootPath           IbsenRootPath
	LogBlocks          []Block
	IndexBlocks        []Block
	TopicWriter        TopicWriter
	TopicIndexerWriter TopicIndexerWriter
}

func (t *TopicHandler) updateFromFileSystem() error {
	logFilesInDirectory, err := ListFilesInDirectory(t.Afs, t.TopicPath(), "log")
	if err != nil {
		return errore.WrapWithContext(err)
	}
	indexFilesInDirectory, err := ListFilesInDirectory(t.Afs, t.TopicPath(), "idx")
	if err != nil {
		return errore.WrapWithContext(err)
	}
	logBlocks, err := FilesToBlocks(logFilesInDirectory)
	t.AddLogBlocks(logBlocks)
	t.sortLogBlocks()
	indexBlocks, err := FilesToBlocks(indexFilesInDirectory)
	t.AddLogBlocks(indexBlocks)
	t.sortLogBlocks()
	return nil
}

func (t *TopicHandler) newBlock(offset Offset) FileName {
	t.AddLogBlock(Block(offset))
	return t.logBlockFileName(Block(offset))
}

func (t *TopicHandler) currentBlock() (FileName, error) {
	size := t.Size()
	if size == 0 {
		return "", BlockNotFound
	}
	offset := t.LogBlocks[size-1]
	fileName := t.logBlockFileName(offset)
	return fileName, nil
}

func (t TopicHandler) findBlockContaining(offset Offset) (FileName, BlockIndex, error) {
	size := t.Size()
	if size == 0 {
		return "", 0, BlockNotFound
	}
	if offset > t.TopicWriter.CurrentOffset {
		return "", 0, BlockNotFound
	}
	lastBlock := len(t.LogBlocks) - 1
	lastOffset := t.LogBlocks[lastBlock]
	if Block(offset) > lastOffset {
		return t.logBlockFileName(lastOffset), BlockIndex(lastBlock), nil
	}
	for i := lastBlock; i >= 0; i-- {
		if Block(offset) < t.LogBlocks[i] {
			return t.logBlockFileName(t.LogBlocks[i]), BlockIndex(i), nil
		}
	}
	return "", 0, BlockNotFound
}

func (t TopicHandler) allBlocksFromOffset(offset Offset) ([]FileName, error) {
	_, index, err := t.findBlockContaining(offset)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	offsets := t.LogBlocks[index:]
	var fileNames []FileName
	for _, block := range offsets {
		fileName := t.logBlockFileName(block)
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil
}

func (t *TopicHandler) IsEmpty() bool {
	return t.LogBlocks == nil || len(t.LogBlocks) == 0
}

func (t TopicHandler) Size() int {
	return len(t.LogBlocks)
}

func (t TopicHandler) LogBlockHead() Block {
	if t.IsEmpty() {
		return 0
	}
	return t.LogBlocks[len(t.LogBlocks)-1]
}

func (t TopicHandler) TopicPath() string {
	return string(t.RootPath) + Separator + string(t.Topic)
}

func (t *TopicHandler) AddLogBlocks(blocks []Block) {
	t.LogBlocks = append(t.LogBlocks, blocks...)
}

func (t *TopicHandler) AddLogBlock(block Block) {
	t.LogBlocks = append(t.LogBlocks, block)
}

func (t *TopicHandler) sortLogBlocks() {
	sort.Slice(t.LogBlocks, func(i, j int) bool { return t.LogBlocks[i] < t.LogBlocks[j] })
}

func (t *TopicHandler) AddIndexBlocks(blocks []Block) {
	t.LogBlocks = append(t.IndexBlocks, blocks...)
}

func (t *TopicHandler) AddIndexBlock(block Block) {
	t.LogBlocks = append(t.IndexBlocks, block)
}

func (t *TopicHandler) sortIndexBlocks() {
	sort.Slice(t.IndexBlocks, func(i, j int) bool { return t.IndexBlocks[i] < t.IndexBlocks[j] })
}

func (t *TopicHandler) logBlockFileName(block Block) FileName {
	fileName := fmt.Sprintf("%020d.%s", block, "log")
	return FileName(string(t.RootPath) + Separator + string(t.Topic) + Separator + fileName)
}

func (t *TopicHandler) indexBlockFileName(block Block) FileName {
	fileName := fmt.Sprintf("%020d.%s", block, "idx")
	return FileName(string(t.RootPath) + Separator + string(t.Topic) + Separator + fileName)
}

func (t TopicHandler) findNotArchivedIndexBlocks() []Block { //Todo: Needs more index validation
	logBlocks := len(t.LogBlocks)
	indexBlocks := len(t.IndexBlocks)
	notIndexed := logBlocks - indexBlocks
	notIndexedFromArrayIndex := (logBlocks - notIndexed) - 1
	notIndexedBlocks := t.LogBlocks[notIndexedFromArrayIndex:]
	if len(notIndexedBlocks) > 1 {
		notIndexedBlocks = notIndexedBlocks[:len(notIndexedBlocks)-1]
	}
	return notIndexedBlocks
}

func (t TopicHandler) UpdateIndex() error {
	for _, offset := range t.findNotArchivedIndexBlocks() {
		logBlockFileName := t.logBlockFileName(offset)
		indexBlockFileName := t.indexBlockFileName(offset)
		err := t.TopicIndexerWriter.createIndexArchive(logBlockFileName, indexBlockFileName)
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	head := t.LogBlockHead()
	if head == 0 {
		return nil
	}
	blockFileName := t.logBlockFileName(head)
	if t.TopicIndexerWriter.isHead(blockFileName) {
		err := t.TopicIndexerWriter.updateHeadIndex()
		if err != nil {
			return errore.WrapWithContext(err)
		}
	} else {
		err := t.TopicIndexerWriter.createNewHeadIndex(blockFileName)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		err = t.TopicIndexerWriter.updateHeadIndex()
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	return nil
}

func (t *TopicHandler) Write(entries Entries, bytes BlockSizeInBytes) (Offset, error) {
	t.logMutex.Lock()
	defer t.logMutex.Unlock()
	if t.TopicWriter.CurrentBlockSize > bytes {
		fileName := t.newBlock(t.TopicWriter.CurrentOffset)
		t.TopicWriter.clearCurrentBlockSize()
		err := t.TopicWriter.Write(fileName, entries)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	} else {
		block, err := t.currentBlock()
		if err != nil {
			return t.TopicWriter.CurrentOffset, errore.WrapWithContext(err)
		}
		err = t.TopicWriter.Write(block, entries)
		if err != nil {
			return t.TopicWriter.CurrentOffset, err
		}
	}
	return t.TopicWriter.CurrentOffset, nil
}

func (t TopicHandler) Read(params ReadParams) (Offset, error) {
	allBlocksFromOffset, err := t.allBlocksFromOffset(params.Offset)
	var lastOffset = params.Offset
	if err != nil {
		return params.Offset, errore.WrapWithContext(err)
	}
	for _, block := range allBlocksFromOffset {
		afsFile, err := OpenFileForRead(t.Afs, string(block))
		if err != nil {
			err := afsFile.Close()
			if err != nil {
				return 0, errore.WrapWithContext(err)
			}
			return params.Offset, errore.WrapWithContext(err)
		}

		lastOffset, err = ReadFileFromLogOffset(afsFile, params)
		if err != nil {
			err := afsFile.Close()
			if err != nil {
				return 0, errore.WrapWithContext(err)
			}
			return params.Offset, errore.WrapWithContext(err)
		}
		err = afsFile.Close()
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}

	}
	return lastOffset, nil
}

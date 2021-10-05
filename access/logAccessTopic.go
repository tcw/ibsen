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
	Afs         *afero.Afero
	mu          *sync.Mutex
	Topic       Topic
	RootPath    IbsenRootPath
	Blocks      []Offset
	TopicWriter TopicWriter
}

func (t *TopicHandler) updateFromFileSystem() error {
	logFilesInDirectory, err := ListFilesInDirectory(t.Afs, t.TopicPath(), "log")
	if err != nil {
		return errore.WrapWithContext(err)
	}
	blocks, err := FilesToBlocks(logFilesInDirectory)
	t.AddBlocks(blocks)
	t.sortBlocks()
	return nil
}

func (t *TopicHandler) newBlock(offset Offset) FileName {
	t.AddBlock(offset)
	return t.blockFileName(offset)
}

func (t *TopicHandler) currentBlock() (FileName, error) {
	size := t.Size()
	if size == 0 {
		return "", BlockNotFound
	}
	offset := t.Blocks[size-1]
	fileName := t.blockFileName(offset)
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
	lastBlock := len(t.Blocks) - 1
	lastOffset := t.Blocks[lastBlock]
	if offset > lastOffset {
		return t.blockFileName(lastOffset), BlockIndex(lastBlock), nil
	}
	for i := lastBlock; i >= 0; i-- {
		if offset < t.Blocks[i] {
			return t.blockFileName(t.Blocks[i]), BlockIndex(i), nil
		}
	}
	return "", 0, BlockNotFound
}

func (t TopicHandler) allBlocksFromOffset(offset Offset) ([]FileName, error) {
	_, index, err := t.findBlockContaining(offset)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	offsets := t.Blocks[index:]
	var fileNames []FileName
	for _, block := range offsets {
		fileName := t.blockFileName(block)
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil
}

func (t *TopicHandler) IsEmpty() bool {
	return t.Blocks == nil || len(t.Blocks) == 0
}

func (t TopicHandler) Size() int {
	return len(t.Blocks)
}

func (t TopicHandler) TopicPath() string {
	return string(t.RootPath) + Separator + string(t.Topic)
}

func (t *TopicHandler) AddBlocks(blocks []Offset) {
	t.Blocks = append(t.Blocks, blocks...)
}

func (t *TopicHandler) AddBlock(block Offset) {
	t.Blocks = append(t.Blocks, block)
}

func (t *TopicHandler) sortBlocks() {
	sort.Slice(t.Blocks, func(i, j int) bool { return t.Blocks[i] < t.Blocks[j] })
}

func (t *TopicHandler) blockFileName(block Offset) FileName {
	fileName := fmt.Sprintf("%020d.%s", block, "log")
	return FileName(string(t.RootPath) + Separator + string(t.Topic) + Separator + fileName)
}

func (t *TopicHandler) Write(entries Entries, bytes BlockSizeInBytes) (Offset, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
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

package access

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"sort"
)

var BlockNotFound = errors.New("block not found")

type Topic struct {
	Afs         *afero.Afero
	Topic       commons.Topic
	RootPath    commons.IbsenRootPath
	Blocks      []commons.Offset
	TopicWriter TopicWriter
}

func (t *Topic) updateFromFileSystem() error {
	logFilesInDirectory, err := ListFilesInDirectory(t.Afs, t.TopicPath(), "log")
	if err != nil {
		return errore.WrapWithContext(err)
	}
	blocks, err := FilesToBlocks(logFilesInDirectory)
	t.AddBlocks(blocks)
	t.sortBlocks()
	return nil
}

func (t *Topic) createNewBlock(offset commons.Offset) commons.FileName {
	t.AddBlock(offset)
	return t.blockFileName(offset)
}

func (t *Topic) currentBlock() (commons.FileName, error) {
	size := t.Size()
	if size == 0 {
		return "", BlockNotFound
	}
	offset := t.Blocks[size-1]
	fileName := t.blockFileName(offset)
	return fileName, nil
}

func (t Topic) findBlockContaining(offset commons.Offset) (commons.FileName, error) {
	size := t.Size()
	if size == 0 {
		return "", BlockNotFound
	}
	if offset > t.TopicWriter.CurrentOffset {
		return "", BlockNotFound
	}
	lastOffset := t.Blocks[len(t.Blocks)-1]
	if offset > lastOffset {
		return t.blockFileName(lastOffset), nil
	}
	for i := len(t.Blocks) - 1; i >= 0; i-- {
		if offset < t.Blocks[i] {
			return t.blockFileName(t.Blocks[i]), nil
		}
	}
	return "", BlockNotFound
}

func (t *Topic) IsEmpty() bool {
	return t.Blocks == nil || len(t.Blocks) == 0
}

func (t Topic) Size() int {
	return len(t.Blocks)
}

func (t Topic) TopicPath() string {
	return string(t.RootPath) + Separator + string(t.Topic)
}

func (t *Topic) AddBlocks(blocks []commons.Offset) {
	t.Blocks = append(t.Blocks, blocks...)
}

func (t *Topic) AddBlock(block commons.Offset) {
	t.Blocks = append(t.Blocks, block)
}

func (t *Topic) sortBlocks() {
	sort.Slice(t.Blocks, func(i, j int) bool { return t.Blocks[i] < t.Blocks[j] })
}

func (t *Topic) blockFileName(block commons.Offset) commons.FileName {
	fileName := fmt.Sprintf("%020d.%s", block, "log")
	return commons.FileName(string(t.RootPath) + Separator + string(t.Topic) + Separator + fileName)
}

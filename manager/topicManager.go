package manager

import (
	"errors"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"sync"
)

type TopicHandler struct {
	afs            *afero.Afero
	rootPath       string
	topic          access.Topic
	maxBlockSize   access.BlockSizeInBytes
	headBlockSize  access.BlockSizeInBytes
	logBlocks      access.Blocks
	logOffset      access.Offset
	logMutex       sync.Mutex
	indexBlocks    access.Blocks
	indexOffset    access.Offset
	indexMutex     sync.Mutex
	logAccess      access.LogAccess
	logIndexAccess access.LogIndexAccess
	loaded         bool
}

func newTopicHandler(asf *afero.Afero, rootPath string, topic access.Topic) TopicHandler {
	return TopicHandler{
		afs:         asf,
		rootPath:    rootPath,
		topic:       topic,
		logBlocks:   access.Blocks{},
		logOffset:   0,
		logMutex:    sync.Mutex{},
		indexBlocks: access.Blocks{},
		indexOffset: 0,
		indexMutex:  sync.Mutex{},
		logAccess: access.ReadWriteLogAccess{
			Afs:      asf,
			RootPath: rootPath,
		},
		logIndexAccess: access.ReadWriteLogIndexAccess{
			Afs:          asf,
			RootPath:     rootPath,
			IndexDensity: 0.01,
		},
		loaded: false,
	}
}

func (t *TopicHandler) Write(entries access.Entries) error {
	t.logMutex.Lock()
	defer t.logMutex.Unlock()
	err := t.Load()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	offset, bytes, err := t.logAccess.Write(t.logBlocks.Head().LogFileName(t.rootPath, t.topic), entries, t.logOffset)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.logOffset = offset
	t.headBlockSize = t.headBlockSize + bytes
	if t.headBlockSize > t.maxBlockSize {
		t.logBlocks.AddBlock(access.Block(t.logOffset))
	}
	return nil
}

func (t *TopicHandler) Read(params access.ReadParams) (access.Offset, error) {
	err := t.Load()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	blocks := t.logBlocks.GetBlocks(t.logOffset)
	if len(blocks) == 0 {
		return 0, errore.WrapWithContext(errors.New("no blocks"))
	}
	var lastReadOffset access.Offset = 0
	for _, block := range blocks {
		logFileName := block.LogFileName(t.rootPath, t.topic)
		lastReadOffset, err = t.logAccess.ReadLog(logFileName, params, 0)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}
	return lastReadOffset, nil
}

func (t *TopicHandler) Load() error {
	if !t.loaded {
		t.logMutex.Lock()
		t.indexMutex.Lock()
		defer t.logMutex.Unlock()
		defer t.indexMutex.Lock()
		if !t.loaded {
			err := t.lazyLoad()
			if err != nil {
				return errore.WrapWithContext(err)
			}
			t.loaded = true
		}
	}
	return nil
}

func (t *TopicHandler) lazyLoad() error {
	blocks, err := t.logAccess.ReadTopicLogBlocks(t.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.logBlocks = blocks
	indexBlocks, err := t.logIndexAccess.ReadTopicIndexBlocks(t.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.indexBlocks = indexBlocks
	return nil
}

func (t *TopicHandler) updateIndex() error {
	blockWithoutIndex := t.logBlocks.Diff(t.indexBlocks)

}

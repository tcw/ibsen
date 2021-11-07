package manager

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"sync"
)

type TopicManager struct {
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
}

func newTopicManager(asf *afero.Afero, rootPath string, topic access.Topic) TopicManager {
	return TopicManager{
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
	}
}

func (t *TopicManager) Write(entries access.Entries) error {
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

func (t *TopicManager) Read(params access.ReadParams) error {
	panic("implement me")
}

func (t *TopicManager) Load() error {
	access.re
}

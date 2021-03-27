package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"sync"
)

type TopicIndexManager struct {
	topicIndexer    *TopicModuloIndex
	blocks          commons.TopicBlocks
	logTopicManager *storage.TopicManager
	IndexingState   *IndexingState
	mu              *sync.Mutex
}

type TopicIndexParams struct {
	afs          *afero.Afero
	topic        string
	rootPath     string
	topicManager *storage.TopicManager
	modulo       uint32
}

func NewTopicIndexManager(params TopicIndexParams) (*TopicIndexManager, error) {
	blocks, err := commons.ListIndexBlocksInTopicOrderedAsc(params.afs, params.rootPath, params.topic)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	topicModuloIndex := &TopicModuloIndex{
		afs:      params.afs,
		rootPath: params.rootPath,
		topic:    params.topic,
		modulo:   params.modulo,
	}
	return &TopicIndexManager{
		topicIndexer:    topicModuloIndex,
		blocks:          blocks,
		logTopicManager: params.topicManager,
		IndexingState:   &IndexingState{},
		mu:              &sync.Mutex{},
	}, nil
}

type IndexingState struct {
	block         uint64
	logOffset     uint64
	logByteOffset uint64
	ByteOffset    commons.ByteOffset
}

func (m *TopicIndexManager) BuildIndex() error {
	indexBlocks := m.blocks
	manager := m.logTopicManager.GetBlockManager(m.topicIndexer.topic)
	logBlocks := manager.GetBlocks()
	toBeIndexed, err := getBlockToBeIndexed(indexBlocks.Blocks, logBlocks)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	m.topicIndexer.
	return nil
}

func (m *TopicIndexManager) DropIndex() (int, error) {
	indexBlocks, err := commons.ListIndexBlocksInTopicOrderedAsc(m.topicIndexer.afs, m.topicIndexer.rootPath, m.topicIndexer.topic)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	droppedIndices := 0
	for _, filename := range indexBlocks.BlockFilePathsOrderedAsc(m.topicIndexer.rootPath) {
		err := m.topicIndexer.afs.Remove(filename)
		if err != nil {
			return droppedIndices, errore.WrapWithContext(err)
		}
		droppedIndices = droppedIndices + 1
	}
	return droppedIndices, nil
}

func (m *TopicIndexManager) FindClosestIndex(offset commons.Offset) InternalIndexOffset {
	return InternalIndexOffset{} //Todo: implement
}

type InternalIndexOffset struct {
	blockIndex int32
	byteOffset int64
}

func getBlockToBeIndexed(indexBlocks []uint64, logBlocks []uint64) ([]uint64, error) {
	idxBlockLength := len(indexBlocks)
	logBlockLength := len(logBlocks)
	if logBlockLength == 0 {
		return []uint64{}, nil
	}
	if idxBlockLength == 0 {
		return logBlocks[idxBlockLength:], nil
	} else {
		return logBlocks[idxBlockLength-1:], nil
	}
}

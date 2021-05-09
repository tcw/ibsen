package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
	"sync"
)

type TopicIndexManager struct {
	topicIndexer    *TopicModuloIndex
	blocks          commons.TopicBlocks
	logTopicManager *storage.TopicManager
	IndexingState   IndexingState
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
	state := IndexingState{}
	blocks, err := commons.ListIndexBlocksInTopicOrderedAsc(params.afs, params.rootPath, params.topic)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	if !blocks.IsEmpty() {
		head, err := blocks.BlockHead()
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		indexBlockFilename := CreateIndexModBlockFilename(params.rootPath, params.topic, head, params.modulo)
		moduloIndex, err := ReadByteOffsetFromFile(params.afs, indexBlockFilename)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		state = IndexingState{
			block:         head,
			logOffset:     moduloIndex.getOffset(),
			logByteOffset: moduloIndex.getByteOffsetHead(),
		}
		log.Printf("found index state for topic %s: %v ", params.topic, state)
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
		IndexingState:   state,
		mu:              &sync.Mutex{},
	}, nil
}

type IndexingState struct {
	block         uint64
	logOffset     commons.Offset
	logByteOffset commons.ByteOffset
}

func (i IndexingState) IsEmpty() bool {
	return i == IndexingState{}
}

func (m *TopicIndexManager) BuildIndex() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.logTopicManager.GetOffset() == 0 {
		return nil
	}
	if m.logTopicManager.GetOffset() == m.IndexingState.logOffset {
		return nil
	}
	indexBlocks := m.blocks
	logBlocks := m.logTopicManager.GetBlocks()
	toBeIndexed, err := getBlocksToBeIndexed(indexBlocks.Blocks, logBlocks)
	log.Println("to be indexed", toBeIndexed)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	m.IndexingState, err = m.topicIndexer.BuildIndex(toBeIndexed, m.IndexingState)
	log.Println(m.IndexingState)
	m.blocks.AddBlocks(toBeIndexed)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (m *TopicIndexManager) DropIndex() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *TopicIndexManager) FindClosestIndex(offset commons.Offset) (commons.IndexedOffset, error) {
	blockContainingOffset, err := m.blocks.FindBlockContaining(offset)
	if err != nil {
		return commons.IndexedOffset{}, errore.WrapWithContext(err)
	}
	indexBlockFilename := CreateIndexModBlockFilename(m.topicIndexer.rootPath, m.topicIndexer.topic, blockContainingOffset, m.topicIndexer.modulo)
	//Todo add LRU caching
	moduloIndex, err := ReadByteOffsetFromFile(m.topicIndexer.afs, indexBlockFilename)
	if err != nil {
		return commons.IndexedOffset{}, errore.WrapWithContext(err)
	}
	byteOffset, err := moduloIndex.getClosestByteOffset(offset)
	if err != nil {
		return commons.IndexedOffset{}, errore.WrapWithContext(err)
	}
	return commons.IndexedOffset{
		Block:      blockContainingOffset,
		ByteOffset: byteOffset,
	}, nil
}

func (m *TopicIndexManager) GetAllIndices() (map[uint64]ModuloIndex, error) {
	var indices map[uint64]ModuloIndex
	indices = make(map[uint64]ModuloIndex)

	for _, block := range m.blocks.Blocks {
		indexBlockFilename := CreateIndexModBlockFilename(m.topicIndexer.rootPath, m.topicIndexer.topic, block, m.topicIndexer.modulo)
		moduloIndex, err := ReadByteOffsetFromFile(m.topicIndexer.afs, indexBlockFilename)
		if err != nil {
			return map[uint64]ModuloIndex{}, errore.WrapWithContext(err)
		}
		indices[block] = moduloIndex
	}
	return indices, nil
}

func getBlocksToBeIndexed(indexBlocks []uint64, logBlocks []uint64) ([]uint64, error) {
	idxBlockLength := len(indexBlocks)
	logBlockLength := len(logBlocks)
	if logBlockLength == 0 {
		return []uint64{}, nil
	}
	if idxBlockLength == 0 {
		log.Println(logBlocks)
		return logBlocks[:], nil
	} else {
		log.Println(logBlocks)
		return logBlocks[idxBlockLength-1:], nil
	}
}

package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
	"sync"
)

type TopicManager struct {
	topic    string
	afs      *afero.Afero
	rootPath string
	mu       sync.Mutex
}

func (m *TopicManager) BuildIndex() error {
	indexBlocks, err := storage.ListIndexBlocksInTopicOrderedAsc(m.afs, m.rootPath, m.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	log.Println(indexBlocks)
	return nil
}

func (m *TopicManager) DropIndex() (int, error) {
	indexBlocks, err := storage.ListIndexBlocksInTopicOrderedAsc(m.afs, m.rootPath, m.topic)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	droppedIndices := 0
	for _, filename := range indexBlocks.BlockFilePathsOrderedAsc(m.rootPath) {
		err := m.afs.Remove(filename)
		if err != nil {
			return droppedIndices, errore.WrapWithContext(err)
		}
		droppedIndices = droppedIndices + 1
	}
	return droppedIndices, nil
}

//StartWatching starts building index from the index instance closets to head
//and builds index for each new log change event until StopWatching is called
func (m *TopicManager) StartWatching() error {
	indexBlocks, err := storage.ListIndexBlocksInTopicOrderedAsc(m.afs, m.rootPath, m.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	_, err = indexBlocks.LastBlockFileName(m.rootPath)
	if err == storage.BlockNotFound {
		//index all blocks
	}
	log.Println(indexBlocks)
	return nil
}

func (m *TopicManager) StopWatching() error {
	return nil
}

func (m *TopicManager) buildBlockIndex(block int64) error {
	logBlockFilename := storage.CreateLogBlockFilename(m.rootPath, m.topic, block)
	logFile, err := storage.OpenFileForRead(m.afs, logBlockFilename)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	_, err = m.findIndexBlockLastByteOffset(block)
	if err != nil {
		return errore.WrapWithContext(err)
	}

	hasMore := true
	for hasMore {
		positions, err := storage.ReadOffsetAndByteOffset(logFile, 1000)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		if positions == nil {
			hasMore = false
		}
	}
	return nil
}

func (m *TopicManager) findIndexBlockLastByteOffset(block int64) (int64, error) {
	indexBlockFilename := storage.CreateIndexBlockFilename(m.rootPath, m.topic, block)
	indexFile, err := storage.OpenFileForRead(m.afs, indexBlockFilename)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return findLastByteOffset(indexFile)
}

func findLastByteOffset(file afero.File) (int64, error) {
	return 0, nil //todo: implement
}

func FindClosestIndex(topic string, offset uint64) InternalIndexOffset {
	return InternalIndexOffset{} //Todo: implement
}

type InternalIndexOffset struct {
	blockIndex int32
	byteOffset int64
}

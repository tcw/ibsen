package index

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
)

type TopicManager struct {
	topic    string
	afs      *afero.Afero
	rootPath string
}

func (m *TopicManager) BuildIndex() error {
	indexBlocks, err := storage.ListIndexBlocksInTopicOrderedAsc(m.afs, m.rootPath, m.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	log.Println(indexBlocks)
	return nil
}

func (m *TopicManager) DropIndex() {

}

func (m *TopicManager) buildBlockIndex(file afero.File) {

}

func buildBlockIndexFromPosition(file afero.File, byteOffset int64) {

}

func FindClosestIndex(topic string, offset uint64) InternalIndexOffset {
	return InternalIndexOffset{} //Todo: implement
}

type InternalIndexOffset struct {
	blockIndex int32
	byteOffset int64
}

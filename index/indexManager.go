package index

import (
	"github.com/spf13/afero"
)

type TopicManager struct {
	topic string
	afs   *afero.Afero
}

func (m *TopicManager) BuildIndex() {

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

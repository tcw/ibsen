package index

import (
	"github.com/tcw/ibsen/storage"
	"os"
)

type Manager struct {
	topicManger storage.TopicManager
}

func BuildIndices() {

}

func BuildIndex(topic string) {

}

func buildBlockIndex(file os.File) {

}

func buildBlockIndexFromPosition(file os.File, byteOffset int64) {

}

func FindClosestIndex(topic string, offset uint64) OffsetIndex {
	return OffsetIndex{} //Todo: implement
}

type OffsetIndex struct {
	blockIndex int32
	byteOffset int64
}

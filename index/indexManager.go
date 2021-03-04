package index

import (
	"github.com/spf13/afero"
)

type Manager struct {
	afs *afero.Afero
}

func (m *Manager) DropIndices() {

}

func (m *Manager) BuildIndices() {

}

func (m *Manager) BuildIndex(topic string) {

}

func (m *Manager) DropIndex(topic string) {

}

func (m *Manager) buildBlockIndex(file afero.File) {

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

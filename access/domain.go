package access

import (
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync"
)

const Sep = string(os.PathSeparator)

type Offset uint64
type Block uint64
type Topic string
type Entries *[][]byte
type BlockSizeInBytes uint64
type FileName string
type BlockIndex uint32
type StrictlyMonotonicOrderedVarIntIndex []byte

func (b Block) LogFileName(rootPath string, topic Topic) FileName {
	return FileName(rootPath + Sep + string(topic) + Sep + fmt.Sprintf("%020d.log", b))
}

func (b Block) IndexFileName(rootPath string, topic Topic) FileName {
	return FileName(rootPath + Sep + string(topic) + Sep + fmt.Sprintf("%020d.idx", b))
}

type Blocks struct {
	BlockList []Block
}

func (bs *Blocks) AddBlock(block Block) {
	bs.BlockList = append(bs.BlockList, block)
}

func (bs Blocks) Head() Block {
	if bs.IsEmpty() {
		return 0
	}
	return bs.BlockList[len(bs.BlockList)-1]
}

func (bs Blocks) IsEmpty() bool {
	return bs.BlockList == nil || len(bs.BlockList) == 0
}

func (bs Blocks) Size() int {
	return len(bs.BlockList)
}

func (bs *Blocks) Sort() {
	sort.Slice(bs.BlockList, func(i, j int) bool { return bs.BlockList[i] < bs.BlockList[j] })
}

func (bs *Blocks) Diff(blocks Blocks) Blocks {
	if blocks.IsEmpty() {
		return Blocks{BlockList: []Block{}}
	}
	if blocks.Size() == 1 {
		return Blocks{BlockList: bs.BlockList[1:]}
	}
	blockList := bs.BlockList[len(blocks.BlockList)-1:]
	return Blocks{BlockList: blockList}

}

func (bs Blocks) GetBlocks(offset Offset) []Block {
	if bs.Size() == 0 {
		return []Block{}
	}
	if bs.Size() == 1 {
		return bs.BlockList[0:]
	}
	for i := bs.Size() - 1; i >= 0; i-- {
		if offset > Offset(bs.BlockList[i]) {
			return bs.BlockList[i:]
		}
	}
	return []Block{}
}

type Index struct {
	IndexOffset []indexOffset
}

func (idx *Index) add(pair indexOffset) {
	idx.IndexOffset = append(idx.IndexOffset, pair)
}

func (idx *Index) addAll(pair []indexOffset) {
	idx.IndexOffset = append(idx.IndexOffset, pair...)
}

// this is linear search, should use range tree for large indices
func (idx Index) findNearestByteOffset(offset Offset) int64 {
	for i := len(idx.IndexOffset) - 1; i >= 0; i-- {
		if offset > idx.IndexOffset[i].Offset {
			return idx.IndexOffset[i].byteOffset
		}
	}
	return 0
}

func (idx *Index) addIndex(index Index) {
	idx.addAll(index.IndexOffset)
}

type indexOffset struct {
	Offset     Offset
	byteOffset int64
}

type LogEntry struct {
	Offset   uint64
	Crc      uint32
	ByteSize int
	Entry    []byte
}

type ReadParams struct {
	Topic     Topic
	Offset    Offset
	BatchSize uint32
	LogChan   chan *[]LogEntry
	Wg        *sync.WaitGroup
}

var crc32q = crc32.MakeTable(crc32.Castagnoli)

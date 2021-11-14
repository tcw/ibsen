package access

import (
	"fmt"
	"sort"
)

type Block uint64
type BlockIndex uint32

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

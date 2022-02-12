package access

import (
	"errors"
	"fmt"
	"sort"
)

type Block uint64
type BlockIndex uint32

var BlockNotFound = errors.New("block not found")
var IndexEntryNotFound = errors.New("index entry not found")

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

func (bs Blocks) Get(index int) Block {
	return bs.BlockList[index]
}

func (bs Blocks) Size() int {
	return len(bs.BlockList)
}

func (bs Blocks) ToString() string {
	list := bs.BlockList
	blocklist := ""
	for i, val := range list {
		blocklist = blocklist + fmt.Sprintf("%d -> %d\n", i, val)
	}
	return blocklist
}

func (bs *Blocks) Sort() {
	sort.Slice(bs.BlockList, func(i, j int) bool { return bs.BlockList[i] < bs.BlockList[j] })
}

func (bs Blocks) Diff(blocks Blocks) Blocks {
	if blocks.IsEmpty() {
		return bs
	}
	if blocks.Size() == 1 {
		return Blocks{BlockList: bs.BlockList[1:]}
	}
	blockList := bs.BlockList[len(blocks.BlockList)-1:]
	return Blocks{BlockList: blockList}
}

func (bs Blocks) Tail() ([]Block, error) {
	if bs.IsEmpty() || bs.Size() < 2 {
		return []Block{}, BlockNotFound
	}
	return bs.BlockList[:bs.Size()-2], nil
}

func (bs Blocks) Contains(offset Offset) (Block, error) {
	if bs.Size() == 0 {
		return 0, BlockNotFound
	}
	if bs.Size() == 1 {
		return bs.BlockList[0], nil
	}
	for i := bs.Size() - 1; i >= 0; i-- {
		if offset >= Offset(bs.BlockList[i]) {
			return bs.BlockList[i], nil
		}
	}
	return 0, BlockNotFound
}

func (bs Blocks) GetBlocksIncludingAndAfter(offset Offset) ([]Block, error) {
	if bs.Size() <= 0 {
		return []Block{}, BlockNotFound
	}
	if bs.Size() == 1 {
		return bs.BlockList[0:], nil
	}
	if offset < Offset(bs.Get(0)) {
		return bs.BlockList[0:], nil
	}
	for i := bs.Size() - 1; i >= 0; i-- {
		if offset >= Offset(bs.BlockList[i]) {
			return bs.BlockList[i:], nil
		}
	}
	return []Block{}, BlockNotFound
}

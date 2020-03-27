package ext4

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type BlockRegistry struct {
	rootPath         string
	topic            string
	blocks           []int64
	maxBlockSize     int64
	currentOffset    uint64
	currentBlockSize int64
}

func newBlockRegistry(rootPath string, topic string, maxBlockSize int64) (BlockRegistry, error) {
	registry := BlockRegistry{
		rootPath:     rootPath,
		topic:        topic,
		maxBlockSize: maxBlockSize,
	}
	err := registry.updateBlocksFromStorage()
	if err != nil {
		return BlockRegistry{}, err
	}
	return registry, nil
}

func (br *BlockRegistry) updateBlocksFromStorage() error {
	var blocks []int64
	files, err := listFilesInDirectory(br.rootPath + separator + br.topic)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		br.blocks = blocks
		br.currentBlockSize = 0
		br.currentOffset = 0
	}
	for _, file := range files {
		splitFileName := strings.Split(file, ".")
		if len(splitFileName) != 2 {
			continue
		}
		if splitFileName[1] == "log" {
			splitPath := strings.Split(splitFileName[0], separator)
			parseUint, err := strconv.ParseInt(splitPath[len(splitPath)-1], 10, 64)
			if err != nil {
				return err
			}
			blocks = append(blocks, parseUint)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	br.blocks = blocks
	blockSize, err := blockSizeFromFilename(br.CurrentBlockFileName())
	if err != nil {
		return err
	}
	br.currentBlockSize = blockSize
	offset, err := findCurrentOffset(br.CurrentBlockFileName())
	if err != nil {
		return err
	}
	br.currentOffset = offset
	return nil
}

func (br *BlockRegistry) IncrementCurrentOffset(increment int) {
	br.currentOffset = br.currentOffset + uint64(increment)
}

func (br *BlockRegistry) IncrementCurrentByteSize(increment int) {
	br.currentBlockSize = br.currentBlockSize + int64(increment)
}

func (br *BlockRegistry) RegisterNewBlock(offset int64) string {
	br.blocks = append(br.blocks, offset)
	br.currentBlockSize = 0
	br.currentOffset = uint64(offset)
	return br.rootPath + separator + br.topic + separator + createBlockFileName(offset)
}

func (br *BlockRegistry) CurrentOffset() uint64 {
	return br.currentOffset
}

func (br *BlockRegistry) CurrentBlock() int64 {
	return br.blocks[len(br.blocks)-1]
}

func (br *BlockRegistry) CurrentBlockFileName() string {
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.CurrentBlock())
}

func (br *BlockRegistry) createBlockFileName(offset int64) string {
	return fmt.Sprintf("%020d.log", offset)
}

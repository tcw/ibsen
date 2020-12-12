package ext4

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"sort"
)

type BlockManager struct {
	asf              *afero.Afero
	rootPath         string
	topic            string
	blocks           []int64
	maxBlockSize     int64
	currentOffset    uint64
	currentBlockSize int64
}

var EndOfBlock = errors.New("end of block")

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func NewBlockManger(afs *afero.Afero, rootPath string, topic string, maxBlockSize int64) (BlockManager, error) {
	registry := BlockManager{
		asf:          afs,
		rootPath:     rootPath,
		topic:        topic,
		maxBlockSize: maxBlockSize,
	}
	err := registry.updateBlocksFromStorage()
	if err != nil {
		return BlockManager{}, errore.WrapWithContext(err)
	}
	return registry, nil
}

func (br *BlockManager) updateBlocksFromStorage() error {
	var blocks []int64
	files, err := listFilesInDirectory(br.asf, br.rootPath+separator+br.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if len(files) == 0 {
		br.blocks = []int64{0}
		br.currentBlockSize = 0
		br.currentOffset = 0
		return nil
	}
	blocks, err = filesToBlocks(files)
	if err != nil {
		return err
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	br.blocks = blocks

	blockSize, err := blockSize(br.asf, br.CurrentBlockFileName())
	if err != nil {
		return errore.WrapWithContext(err)
	}
	br.currentBlockSize = blockSize

	offset, err := findLastOffset(br.asf, br.CurrentBlockFileName())
	if err != nil {
		return errore.WrapWithContext(err)
	}
	br.currentOffset = uint64(offset)

	return nil
}

func (br *BlockManager) incrementCurrentOffset(increment int) {
	br.currentOffset = br.currentOffset + uint64(increment)
}

func (br *BlockManager) incrementCurrentByteSize(increment int) {
	br.currentBlockSize = br.currentBlockSize + int64(increment)
}

func (br *BlockManager) createNewBlock() {
	br.blocks = append(br.blocks, int64(br.currentOffset))
	br.currentBlockSize = 0
}

func (br *BlockManager) CurrentOffset() uint64 {
	return br.currentOffset
}

func (br *BlockManager) CurrentBlock() int64 {
	if br.blocks == nil {
		return 0
	}
	return br.blocks[len(br.blocks)-1]
}

func (br *BlockManager) FirstBlock() int64 {
	return br.blocks[0]
}

func (br *BlockManager) FirstBlockFileName() string {
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.CurrentBlock())
}

func (br *BlockManager) GetBlock(blockIndex int) (int64, error) {
	if blockIndex >= len(br.blocks) {
		return 0, EndOfBlock
	}
	return br.blocks[blockIndex], nil
}

func (br *BlockManager) GetBlockFilename(blockIndex int) (string, error) {
	if blockIndex >= len(br.blocks) {
		return "", EndOfBlock
	}
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.blocks[blockIndex]), nil
}

func (br *BlockManager) CurrentBlockFileName() string {
	path := br.rootPath + separator + br.topic + separator + createBlockFileName(br.CurrentBlock())
	return path
}

func (br *BlockManager) findBlockIndexContainingOffset(offset uint64) (uint, error) {

	if len(br.blocks) == 0 {
		return 0, errore.NewWithContext("No blocks")
	}
	if len(br.blocks) == 1 {
		return 0, nil
	}

	for i, v := range br.blocks {
		if v > int64(offset) {
			return uint(i - 1), nil
		}
	}
	return uint(len(br.blocks) - 1), nil
}

func (br *BlockManager) createBlockFileName(offset int64) string {
	return fmt.Sprintf("%020d.log", offset)
}

func (br *BlockManager) WriteBatch(logEntry [][]byte) error {
	if br.currentBlockSize > br.maxBlockSize {
		br.createNewBlock()
	}
	writer := BlockWriterParams{
		Afs:       br.asf,
		Filename:  br.CurrentBlockFileName(),
		LogEntry:  logEntry,
		offset:    br.currentOffset,
		blockSize: br.currentBlockSize,
	}
	offset, blockSize, err := writer.WriteBatch()
	br.currentOffset = offset
	br.currentBlockSize = blockSize
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

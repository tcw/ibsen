package storage

import (
	"errors"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"sort"
)

type BlockManager struct {
	asf          *afero.Afero
	rootPath     string
	topic        string
	blocks       []int64
	maxBlockSize int64
	offset       uint64
	blockSize    int64
}

var BlockNotFound = errors.New("block not found")

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func NewBlockManger(afs *afero.Afero, rootPath string, topic string, maxBlockSize int64) (BlockManager, error) {
	manager := BlockManager{
		asf:          afs,
		rootPath:     rootPath,
		topic:        topic,
		maxBlockSize: maxBlockSize,
	}

	err := manager.loadBlockStatusFromStorage()
	if err != nil {
		return BlockManager{}, errore.WrapWithContext(err)
	}
	return manager, nil
}

func (br *BlockManager) GetBlockFilename(blockIndex int) (string, error) {
	if blockIndex >= len(br.blocks) {
		return "", BlockNotFound
	}
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.blocks[blockIndex]), nil
}

func (br *BlockManager) FindBlockIndexContainingOffset(offset uint64) (uint, error) {
	if len(br.blocks) == 0 {
		return 0, BlockNotFound
	}
	if br.offset < offset {
		return 0, BlockNotFound
	}

	for i, v := range br.blocks {
		if v > int64(offset) {
			return uint(i - 1), nil
		}
	}
	return uint(len(br.blocks) - 1), nil
}

func (br *BlockManager) HasNextBlock(blockIndex int) bool {
	return blockIndex < len(br.blocks)
}

func (br *BlockManager) WriteBatch(logEntry [][]byte) error {
	if br.blockSize > br.maxBlockSize {
		br.createNewBlock()
	}
	blockFileName, err := br.currentBlockFileName()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	writer := BlockWriterParams{
		Afs:       br.asf,
		Filename:  blockFileName,
		LogEntry:  logEntry,
		offset:    br.offset,
		blockSize: br.blockSize,
	}
	offset, blockSize, err := writer.WriteBatch()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	br.offset = offset
	br.blockSize = blockSize
	return nil
}

func (br *BlockManager) currentBlockFileName() (string, error) {
	filename, err := br.GetBlockFilename(br.lastBlockIndex())
	if err != nil {
		return "", err
	}
	return filename, nil
}

func (br *BlockManager) loadBlockStatusFromStorage() error {

	hasBlocks, sortedBlocks, err := br.findAllBlocksInTopicOrderedAsc()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if !hasBlocks {
		br.setInitBlock()
		return nil
	}

	err = br.setCurrentState(sortedBlocks)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (br *BlockManager) setCurrentState(sortedBlocks []int64) error {
	br.blocks = sortedBlocks
	blockFileName, err := br.currentBlockFileName()
	if err != nil {
		return errore.WrapWithContext(err)
	}

	sizeOfLastBlock, err := blockSize(br.asf, blockFileName)
	if err != nil {
		return errore.WrapWithContext(err)
	}

	offset, err := findLastOffset(br.asf, blockFileName)
	if err != nil {
		return errore.WrapWithContext(err)
	}

	br.blockSize = sizeOfLastBlock
	br.offset = uint64(offset)
	return nil
}

func (br *BlockManager) findAllBlocksInTopicOrderedAsc() (bool, []int64, error) {
	var blocks []int64
	files, err := listFilesInDirectoryRecursively(br.asf, br.rootPath+separator+br.topic)
	if err != nil {
		return false, blocks, errore.WrapWithContext(err)
	}
	if len(files) == 0 {
		return false, blocks, nil
	}
	blocks, err = filesToBlocks(files)
	if err != nil {
		return false, blocks, errore.WrapWithContext(err)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return true, blocks, nil
}

func (br *BlockManager) setInitBlock() {
	br.blocks = []int64{0}
	br.blockSize = 0
	br.offset = 0
}

func (br *BlockManager) createNewBlock() {
	br.blocks = append(br.blocks, int64(br.offset))
	br.blockSize = 0
}

func (br *BlockManager) lastBlock() int64 {
	if br.blocks == nil {
		return 0
	}
	return br.blocks[len(br.blocks)-1]
}

func (br *BlockManager) lastBlockIndex() int {
	if br.blocks == nil {
		return 0
	}
	return len(br.blocks) - 1
}

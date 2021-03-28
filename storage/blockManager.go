package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/messaging"
	"hash/crc32"
)

type BlockManager struct {
	asf          *afero.Afero
	rootPath     string
	topic        string
	blocks       []uint64
	maxBlockSize int64
	offset       uint64
	blockSize    int64
	offsetChange chan uint64
}

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func NewBlockManger(afs *afero.Afero, rootPath string, topic string, maxBlockSize int64) (BlockManager, error) {
	manager := BlockManager{
		asf:          afs,
		rootPath:     rootPath,
		topic:        topic,
		maxBlockSize: maxBlockSize,
		offsetChange: make(chan uint64),
	}

	err := manager.loadBlockStatusFromStorage()
	if err != nil {
		return BlockManager{}, errore.WrapWithContext(err)
	}
	go changeEventDispatch(&manager)
	return manager, nil
}

func changeEventDispatch(manager *BlockManager) {
	for {
		offset := <-manager.offsetChange
		messaging.Publish(messaging.Event{
			Data: messaging.TopicChange{
				Topic:  manager.topic,
				Offset: offset,
			},
			Type: messaging.TopicChangeEventType,
		})
	}
}

func (br *BlockManager) GetBlocks() []uint64 {
	return br.blocks
}

func (br *BlockManager) GetBlockFilename(blockIndex int) (string, error) {
	if blockIndex >= len(br.blocks) {
		return "", commons.BlockNotFound
	}
	return br.rootPath + commons.Separator + br.topic + commons.Separator + commons.CreateBlockFileName(br.blocks[blockIndex], "log"), nil
}

func (br *BlockManager) FindBlockIndexContainingOffset(offset uint64) (uint, error) {
	if len(br.blocks) == 0 {
		return 0, commons.BlockNotFound
	}
	if br.offset < offset {
		return 0, commons.BlockNotFound
	}

	for i, v := range br.blocks {
		if v > offset {
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
	select {
	case br.offsetChange <- offset:
	default:
	}
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

	blocks, err := commons.ListLogBlocksInTopicOrderedAsc(br.asf, br.rootPath, br.topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if blocks.IsEmpty() {
		br.setInitBlock()
		return nil
	}

	err = br.setCurrentState(blocks.Blocks)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (br *BlockManager) setCurrentState(sortedBlocks []uint64) error {
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

func (br *BlockManager) setInitBlock() {
	br.blocks = []uint64{0}
	br.blockSize = 0
	br.offset = 0
}

func (br *BlockManager) createNewBlock() {
	br.blocks = append(br.blocks, br.offset)
	br.blockSize = 0
}

func (br *BlockManager) lastBlock() uint64 {
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

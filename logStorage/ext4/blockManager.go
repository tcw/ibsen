package ext4

import (
	"errors"
	"fmt"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"os"
	"sort"
)

type BlockManager struct {
	rootPath         string
	topic            string
	blocks           []int64
	maxBlockSize     int64
	currentOffset    uint64
	currentBlockSize int64
}

var EndOfBlock = errors.New("end of block")

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func NewBlockManger(rootPath string, topic string, maxBlockSize int64) (BlockManager, error) {
	registry := BlockManager{
		rootPath:     rootPath,
		topic:        topic,
		maxBlockSize: maxBlockSize,
	}
	err := registry.updateBlocksFromStorage()
	if err != nil {
		return BlockManager{}, err
	}
	return registry, nil
}

func (br *BlockManager) updateBlocksFromStorage() error {
	var blocks []int64
	files, err := listFilesInDirectory(br.rootPath + separator + br.topic)
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

	blockSize, err := blockSize(br.CurrentBlockFileName())
	if err != nil {
		return errore.WrapWithContext(err)
	}
	br.currentBlockSize = blockSize

	offset, err := findLastOffset(br.CurrentBlockFileName())
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

func (br *BlockManager) WriteBatch(logEntry *[][]byte) error {
	if br.currentBlockSize > br.maxBlockSize {
		br.createNewBlock()
	}
	writer, err := OpenFileForWrite(br.CurrentBlockFileName())
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = br.writeBatchToFile(writer, logEntry)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = writer.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (br *BlockManager) writeBatchToFile(file *os.File, logEntry *[][]byte) error {
	var bytes []byte
	for _, v := range *logEntry {
		br.incrementCurrentOffset(1)
		bytes = append(bytes, createByteEntry(br, v)...)
	}
	n, err := file.Write(bytes)
	br.incrementCurrentByteSize(n)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func createByteEntry(br *BlockManager, entry []byte) []byte {
	offset := offsetToLittleEndian(br.currentOffset)
	byteSize := byteSizeToLittleEndian(len(entry))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := uint32ToLittleEndian(checksum)
	bytes := append(offset, check...)
	bytes = append(bytes, byteSize...)
	bytes = append(bytes, entry...)
	return bytes
}

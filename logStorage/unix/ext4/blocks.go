package ext4

import (
	"errors"
	"fmt"
	"os"
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

var EndOfBlock = errors.New("End of block")

func NewBlockRegistry(rootPath string, topic string, maxBlockSize int64) (BlockRegistry, error) {
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
		blocks = []int64{0}
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

func (br *BlockRegistry) incrementCurrentOffset(increment int) {
	br.currentOffset = br.currentOffset + uint64(increment)
}

func (br *BlockRegistry) incrementCurrentByteSize(increment int) {
	br.currentBlockSize = br.currentBlockSize + int64(increment)
}

func (br *BlockRegistry) createNewBlock() {
	br.blocks = append(br.blocks, int64(br.currentOffset))
	br.currentBlockSize = 0
}

func (br *BlockRegistry) CurrentOffset() uint64 {
	return br.currentOffset
}

func (br *BlockRegistry) CurrentBlock() int64 {
	if br.blocks == nil {
		return 0
	}
	return br.blocks[len(br.blocks)-1]
}

func (br *BlockRegistry) FirstBlock() int64 {
	return br.blocks[0]
}

func (br *BlockRegistry) FirstBlockFileName() string {
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.CurrentBlock())
}

func (br *BlockRegistry) GetBlock(blockIndex int) (int64, error) {
	if blockIndex >= len(br.blocks) {
		return 0, EndOfBlock
	}
	return br.blocks[blockIndex], nil
}

func (br *BlockRegistry) GetBlockFilename(blockIndex int) (string, error) {
	if blockIndex >= len(br.blocks) {
		return "", EndOfBlock
	}
	return br.rootPath + separator + br.topic + separator + createBlockFileName(br.blocks[blockIndex]), nil
}

func (br *BlockRegistry) CurrentBlockFileName() string {
	path := br.rootPath + separator + br.topic + separator + createBlockFileName(br.CurrentBlock())
	return path
}

func (br *BlockRegistry) findBlockIndexContainingOffset(offset uint64) (uint, error) {

	if len(br.blocks) == 0 {
		return 0, errors.New("no block")
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

func (br *BlockRegistry) createBlockFileName(offset int64) string {
	return fmt.Sprintf("%020d.log", offset)
}

func (br *BlockRegistry) newBlockWriter() (*os.File, error) {
	f, err := os.OpenFile(br.CurrentBlockFileName(),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (br *BlockRegistry) WriteBatch(logEntry *[][]byte) error {
	if br.currentBlockSize > br.maxBlockSize {
		br.createNewBlock()
	}
	writer, err := br.newBlockWriter()
	if err != nil {
		return err
	}
	err = br.writeBatchToFile(writer, logEntry)
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (br *BlockRegistry) writeBatchToFile(file *os.File, logEntry *[][]byte) error {
	var bytes []byte
	for _, v := range *logEntry {
		br.incrementCurrentOffset(1)
		bytes = append(bytes, offsetToLittleEndian(br.currentOffset)...)
		bytes = append(bytes, byteSizeToLittleEndian(len(v))...)
		bytes = append(bytes, v...)
	}
	n, err := file.Write(bytes)
	br.incrementCurrentByteSize(n)
	if err != nil {
		return err
	}
	return nil
}

func (br *BlockRegistry) Write(entry []byte) error {
	if br.currentBlockSize > br.maxBlockSize {
		br.createNewBlock()
	}
	writer, err := br.newBlockWriter()
	if err != nil {
		return err
	}
	err = br.writeToFile(writer, entry)
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (br *BlockRegistry) writeToFile(file *os.File, entry []byte) error {
	br.incrementCurrentOffset(1)
	bytes := append(offsetToLittleEndian(br.currentOffset), byteSizeToLittleEndian(len(entry))...)
	bytes = append(bytes, entry...)
	n, err := file.Write(bytes)
	if err != nil {
		return err
	}
	br.incrementCurrentByteSize(n)
	return nil
}

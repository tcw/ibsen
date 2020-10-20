package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type TopicReader struct {
	blockManager      *BlockManager
	currentOffset     uint64
	currentBlockIndex uint
}

func NewTopicRead(manager *BlockManager) (*TopicReader, error) {

	return &TopicReader{
		blockManager:      manager,
		currentOffset:     0,
		currentBlockIndex: 0,
	}, nil
}

func (t *TopicReader) ReadFromBeginning(c chan logStorage.LogEntry, wg *sync.WaitGroup) error {
	return t.ReadFromBlock(c, wg, 0)
}

func (t *TopicReader) ReadFromBlock(c chan logStorage.LogEntry, wg *sync.WaitGroup, block int) error {
	blockIndex := block
	for {
		filename, err := t.blockManager.GetBlockFilename(blockIndex)
		if err != nil && err != EndOfBlock {
			return err
		}
		if err == EndOfBlock {
			return nil
		}
		read, err := OpenFileForRead(filename)
		if err != nil {
			read.Close()
			return err
		}
		err = ReadLogToEnd(read, c, wg)
		if err != nil {
			read.Close()
			return err
		}
		err = read.Close()
		if err != nil {
			return err
		}
		blockIndex = blockIndex + 1
	}
}

func (t *TopicReader) ReadLogFromOffsetNotIncluding(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, offset uint64) error {
	if t.currentOffset == offset {
		return nil
	}
	currentBlockIndex, err := t.blockManager.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return err
	}
	file, err := OpenFileForRead(blockFileName)
	err = ReadLogFromOffsetNotIncluding(file, logChan, wg, offset)
	if err != nil {
		return err
	}
	return t.ReadFromBlock(logChan, wg, blockIndex+1)
}

func (t *TopicReader) ReadBatchFromBeginning(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int) error {
	return t.ReadBatchFromBlock(c, wg, batchSize, 0)
}

func (t *TopicReader) ReadBatchFromBlock(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int, block int) error {
	blockIndex := block
	var entriesBytes []logStorage.LogEntry
	for {
		filename, err := t.blockManager.GetBlockFilename(blockIndex)
		if err != nil && err != EndOfBlock {
			return err
		}
		if err == EndOfBlock && entriesBytes != nil {
			wg.Add(1)
			c <- logStorage.LogEntryBatch{Entries: entriesBytes}
			return nil
		}
		if err == EndOfBlock {
			return nil
		}
		read, err := OpenFileForRead(filename)
		if err != nil {
			read.Close()
			return err
		}
		partial, hasSent, err := ReadLogInBatchesToEnd(read, entriesBytes, c, wg, batchSize)
		if err != nil {
			return err
		}
		if hasSent {
			entriesBytes = nil
		}
		entriesBytes = append(entriesBytes, partial.Entries...)
		err = read.Close()
		if err != nil {
			return err
		}
		blockIndex = blockIndex + 1
	}
}

func (t *TopicReader) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, offset uint64, batchSize int) error {
	currentBlockIndex, err := t.blockManager.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return err
	}
	file, err := OpenFileForRead(blockFileName)
	err = ReadLogBlockFromOffsetNotIncluding(file, logChan, wg, offset, batchSize)
	if err != nil {
		return err
	}
	return t.ReadBatchFromBlock(logChan, wg, batchSize, blockIndex+1)
}

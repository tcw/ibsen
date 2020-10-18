package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type TopicRead struct {
	blockReg          *BlockRegistry
	currentOffset     uint64
	currentBlockIndex uint
}

func NewTopicRead(rootPath string, topicName string, maxBlockSize int64) (*TopicRead, error) {
	registry, err := NewBlockRegistry(rootPath, topicName, maxBlockSize)
	if err != nil {
		return &TopicRead{}, err
	}

	return &TopicRead{
		blockReg:          &registry,
		currentOffset:     0,
		currentBlockIndex: 0,
	}, nil
}

func (t *TopicRead) ReadFromBeginning(c chan logStorage.LogEntry, wg *sync.WaitGroup) error {
	return t.ReadFromBlock(c, wg, 0)
}

func (t *TopicRead) ReadFromBlock(c chan logStorage.LogEntry, wg *sync.WaitGroup, block int) error {
	blockIndex := block
	for {
		filename, err := t.blockReg.GetBlockFilename(blockIndex)
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

func (t *TopicRead) ReadLogFromOffsetNotIncluding(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, offset uint64) error {
	if t.currentOffset == offset {
		return nil
	}
	currentBlockIndex, err := t.blockReg.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockReg.GetBlockFilename(blockIndex)
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

func (t *TopicRead) ReadBatchFromBeginning(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int) error {
	return t.ReadBatchFromBlock(c, wg, batchSize, 0)
}

func (t *TopicRead) ReadBatchFromBlock(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int, block int) error {
	blockIndex := block
	var entriesBytes []logStorage.LogEntry
	for {
		filename, err := t.blockReg.GetBlockFilename(blockIndex)
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

func (t *TopicRead) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, offset uint64, batchSize int) error {
	currentBlockIndex, err := t.blockReg.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockReg.GetBlockFilename(blockIndex)
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

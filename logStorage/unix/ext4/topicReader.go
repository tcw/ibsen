package ext4

import (
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type TopicRead struct {
	blockReg          *BlockRegistry
	currentOffset     uint64
	currentBlockIndex uint
}

func NewTopicRead(rootPath string, topicName string, topicBlocksSorted *map[string][]int64) (TopicRead, error) {
	topicRead := TopicRead{
		rootPath:  rootPath,
		name:      topicName,
		topicPath: rootPath + separator + topicName,
	}
	topicRead.sortedBlocks = topicBlocksSorted
	return topicRead, nil
}

func (t *TopicRead) ReadFromBeginning(c chan logStorage.LogEntry, wg *sync.WaitGroup) error {
	var currenteBlock uint = 0

	for {
		reader, err := t.createBlockReader(currenteBlock)
		if err != nil {
			return nil
		}
		if reader == nil {
			return nil
		}
		err = reader.ReadLogToEnd(c, wg)
		if err != nil {
			return err
		}
		err = reader.CloseLogReader()
		if err != nil {
			return err
		}
		currenteBlock = currenteBlock + 1
	}
}

func (t *TopicRead) ReadLogFromOffsetNotIncluding(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, offset uint64) error {
	blockIndexContainingOffset, err := t.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	reader, err := t.createBlockReader(blockIndexContainingOffset)
	if err != nil {
		return err
	}
	err = reader.ReadLogFromOffsetNotIncluding(logChan, wg, offset)
	if err != nil {
		return err
	}
	for {
		blockIndexContainingOffset = blockIndexContainingOffset + 1
		isNextblock, err := t.createBlockReader(blockIndexContainingOffset)
		if err != nil {
			return err
		}
		if isNextblock == nil {
			return nil
		}
		err = reader.ReadLogToEnd(logChan, wg)
		if err != nil {
			return err
		}
		err = reader.CloseLogReader()
		if err != nil {
			return err
		}

	}
}

func (t *TopicRead) findBlockIndexContainingOffset(offset uint64) (uint, error) {
	sortedBlocks := (*t.sortedBlocks)[t.name]
	if len(sortedBlocks) == 0 {
		return 0, errors.New("no block")
	}
	if len(sortedBlocks) == 1 {
		return 0, nil
	}

	for i, v := range sortedBlocks {
		if v > int64(offset) {
			return uint(i - 1), nil
		}
	}
	return uint(len(sortedBlocks) - 1), nil
}

//Todo: Create common cache with writer
func (t *TopicRead) createBlockReader(blockIndex uint) (*LogFile, error) {
	sortedBlocks := (*t.sortedBlocks)[t.name]
	if uint(len(sortedBlocks)) <= blockIndex {
		return nil, nil
	}
	blockName := createBlockFileName(sortedBlocks[blockIndex])
	reader, err := NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (t *TopicRead) ReadBatchFromBeginning(c chan logStorage.LogEntryBatch, wg *sync.WaitGroup, batchSize int) error {
	var currenteBlock uint = 0

	var entriesBytes []logStorage.LogEntry
	for {
		reader, err := t.createBlockReader(currenteBlock)
		if err != nil {
			return nil
		}
		if reader == nil && entriesBytes != nil {
			wg.Add(1)
			c <- logStorage.LogEntryBatch{Entries: entriesBytes}
			return nil
		}
		if reader == nil {
			return nil
		}
		partial, hasSent, err := reader.ReadLogInBatchesToEnd(entriesBytes, c, wg, batchSize)
		if err != nil {
			return err
		}
		if hasSent {
			entriesBytes = nil
		}
		entriesBytes = append(entriesBytes, partial.Entries...)
		err = reader.CloseLogReader()
		if err != nil {
			return err
		}
		currenteBlock = currenteBlock + 1
	}
}

func (t *TopicRead) ReadBatchFromOffsetNotIncluding(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, offset uint64, batchSize int) error {
	blockIndexContainingOffset, err := t.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	var entriesBytes []logStorage.LogEntry
	reader, err := t.createBlockReader(blockIndexContainingOffset)
	entries, _, err := reader.ReadLogBlockFromOffsetNotIncluding(logChan, wg, offset, batchSize)
	if err != nil {
		return err
	}
	entriesBytes = append(entriesBytes, entries.Entries...)

	for {
		blockIndexContainingOffset = blockIndexContainingOffset + 1
		reader, err := t.createBlockReader(blockIndexContainingOffset)
		if err != nil {
			return err
		}
		if reader == nil {
			logChan <- logStorage.LogEntryBatch{Entries: entriesBytes}
			return nil
		}
		entries, hasSent, err := reader.ReadLogInBatchesToEnd(entriesBytes, logChan, wg, batchSize)
		if hasSent {
			entriesBytes = nil
		}
		entriesBytes = append(entriesBytes, entries.Entries...)
		if err != nil {
			return err
		}
		err = reader.CloseLogReader()
		if err != nil {
			return err

		}
	}
}

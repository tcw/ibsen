package ext4

import (
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"sync"
)

type TopicRead struct {
	rootPath          string
	name              string
	topicPath         string
	currentOffset     uint64
	sortedBlocks      []uint64
	currentBlockIndex uint
	logFile           *LogFile
}

func ListTopics(rootPath string) ([]string, error) {
	return listUnhiddenDirectories(rootPath)
}

func NewTopicRead(rootPath string, topicName string) (TopicRead, error) {
	topicRead := TopicRead{
		rootPath:          rootPath,
		name:              topicName,
		topicPath:         rootPath + separator + topicName,
		currentBlockIndex: 0,
		currentOffset:     1,
	}
	sorted, err := listBlocksSorted(topicRead.topicPath)
	if err != nil {
		return TopicRead{}, err
	}
	topicRead.sortedBlocks = sorted
	return topicRead, nil
}

func (t *TopicRead) ReadFromBeginning(c chan *logStorage.LogEntry, wg *sync.WaitGroup) error {
	reader, err := NewLogReader(t.rootPath + separator + t.name + separator + createBlockFileName(0))
	if err != nil {
		return err
	}
	t.logFile = reader
	for {
		err := t.logFile.ReadLogFromBeginning(c, wg)
		if err != nil {
			return err
		}
		err = t.logFile.CloseLogReader()
		if err != nil {
			return err
		}
		isNextBlock, err := t.nextBlock()
		if err != nil {
			return nil
		}
		if !isNextBlock {
			return nil
		}
	}
}

func (t *TopicRead) ReadLogFromOffsetNotIncluding(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, offset uint64) error {
	blockIndexContainingOffset, err := t.findBlockIndexContainingOffset(offset)
	if err != nil {
		return err
	}
	t.currentBlockIndex = blockIndexContainingOffset
	t.currentOffset = offset
	blockName := createBlockFileName(t.sortedBlocks[t.currentBlockIndex])
	reader, err := NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	if err != nil {
		return err
	}
	t.logFile = reader
	err = t.logFile.ReadLogFromOffsetNotIncluding(logChan, t.currentOffset) // Todo: add waitgroup
	if err != nil {
		return err
	}
	for {
		err := t.logFile.ReadLogFromBeginning(logChan, wg)
		if err != nil {
			return err
		}
		err = t.logFile.CloseLogReader()
		if err != nil {
			return err
		}
		isNextblock, err := t.nextBlock()
		if err != nil {
			return err
		}
		if !isNextblock {
			return nil
		}
	}
}

func (t *TopicRead) findBlockIndexContainingOffset(offset uint64) (uint, error) {
	if len(t.sortedBlocks) == 0 {
		return 0, errors.New("no block")
	}
	if len(t.sortedBlocks) == 1 {
		return 0, nil
	}

	for i, v := range t.sortedBlocks {
		if v > offset {
			return uint(i - 1), nil
		}
	}
	return uint(len(t.sortedBlocks) - 1), nil
}

//Todo: Create common cache with writer
func (t *TopicRead) nextBlock() (bool, error) {
	t.currentBlockIndex = t.currentBlockIndex + 1
	if uint(len(t.sortedBlocks)) <= t.currentBlockIndex {
		return false, nil
	}
	blockName := createBlockFileName(t.sortedBlocks[t.currentBlockIndex])
	reader, err := NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	if err != nil {
		return false, err
	}
	t.logFile = reader
	return true, nil
}

func (t *TopicRead) ReadBatchFromOffsetNotIncluding(batch *logStorage.EntryBatch, entriesToRead int) (*logStorage.EntryBatchResponse, error) {
	blockIndexContainingOffset, err := t.findBlockIndexContainingOffset(batch.Offset)
	if err != nil {
		return nil, err
	}
	t.currentBlockIndex = blockIndexContainingOffset
	t.currentOffset = batch.Offset
	blockName := createBlockFileName(t.sortedBlocks[t.currentBlockIndex])
	var entriesBytes [][]byte
	reader, err := NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	t.logFile = reader
	entries, err := t.logFile.ReadLogBlockFromOffsetNotIncluding(batch, entriesToRead)
	entriesBytes = append(entriesBytes, *entries.Entries...)
	size := entries.Size()
	if size == entriesToRead {
		return &logStorage.EntryBatchResponse{
			NextBatch: logStorage.EntryBatch{
				Topic:  batch.Topic,
				Offset: 0,
				Marker: 0,
			},
			Entries: &entriesBytes,
		}, nil
	}
	for {
		entries, err := t.logFile.ReadLogToEnd()
		entriesBytes = append(entriesBytes, *entries.Entries...)
		if err != nil {
			return nil, err
		}
		err = t.logFile.CloseLogReader()
		if err != nil {
			return nil, err

		}
		isNextblock, err := t.nextBlock()
		if err != nil {
			return nil, err

		}
		if !isNextblock {
			return &logStorage.EntryBatchResponse{
				NextBatch: logStorage.EntryBatch{
					Topic:  batch.Topic,
					Offset: 0,
					Marker: 0,
				},
				Entries: &entriesBytes,
			}, nil
		}
	}

	return nil, nil
}

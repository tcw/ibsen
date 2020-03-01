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
	currentOffset     logStorage.Offset
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
	t.logFile = NewLogReader(t.rootPath + separator + t.name + separator + createBlockFileName(0))
	for {
		err := t.logFile.ReadLogFromBeginning(c, wg)
		if err != nil {
			return err
		}
		t.logFile.CloseLogReader()
		if !t.nextBlock() {
			return nil
		}
	}
}

func (t *TopicRead) ReadLogFromOffsetNotIncluding(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, offset logStorage.Offset) error {
	blockIndexContainingOffset, err := t.findBlockIndexContainingOffset(uint64(offset))
	if err != nil {
		return err
	}
	t.currentBlockIndex = blockIndexContainingOffset
	t.currentOffset = offset
	blockName := createBlockFileName(t.sortedBlocks[t.currentBlockIndex])
	t.logFile = NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	err = t.logFile.ReadLogFromOffsetNotIncluding(logChan, t.currentOffset) // Todo: add waitgroup
	if err != nil {
		return err
	}
	for {
		err := t.logFile.ReadLogFromBeginning(logChan, wg)
		if err != nil {
			return err
		}
		t.logFile.CloseLogReader()
		if !t.nextBlock() {
			return nil
		}
	}
}

func (t *TopicRead) findBlockIndexContainingOffset(offset uint64) (uint, error) {
	if len(t.sortedBlocks) == 0 {
		return 0, errors.New("No Block")
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
func (t *TopicRead) nextBlock() bool {
	t.currentBlockIndex = t.currentBlockIndex + 1
	if uint(len(t.sortedBlocks)) <= t.currentBlockIndex {
		return false
	}
	blockName := createBlockFileName(t.sortedBlocks[t.currentBlockIndex])
	t.logFile = NewLogReader(t.rootPath + separator + t.name + separator + blockName)
	return true
}

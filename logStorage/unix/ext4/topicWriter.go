package ext4

import (
	"errors"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"sync"
)

type TopicWrite struct {
	rootPath         string
	name             string
	topicPath        string
	currentBlock     uint64
	currentOffset    uint64
	currentBlockSize int64
	maxBlockSize     int64
	logFile          *LogFile
	mu               sync.Mutex
}

func (t *TopicWrite) WriteBatchToTopic(entry *[][]byte) (int, error) {
	t.currentOffset = t.currentOffset + 1
	logEntry := &logStorage.LogBatchEntry{
		Offset:  t.currentOffset,
		Entries: entry,
	}
	offset, n, err := t.logFile.WriteBatchToFile(logEntry)
	t.currentOffset = offset
	if err != nil {
		return 0, err
	}
	t.currentBlockSize = t.currentBlockSize + int64(n)
	if t.currentBlockSize > t.maxBlockSize {
		err := t.createNextBlock()
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (t *TopicWrite) WriteToTopic(entry *[]byte) (int, error) {
	t.currentOffset = t.currentOffset + 1
	logEntry := &logStorage.LogEntry{
		Offset:   t.currentOffset,
		ByteSize: len(*entry),
		Entry:    entry,
	}
	n, err := t.logFile.WriteToFile(logEntry)
	if err != nil {
		return 0, err
	}
	t.currentBlockSize = t.currentBlockSize + int64(n)
	if t.currentBlockSize > t.maxBlockSize {
		err := t.createNextBlock()
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func NewTopicWrite(rootPath string, name string, maxBlockSize int64) (*TopicWrite, error) {

	topic := &TopicWrite{
		rootPath:         rootPath,
		name:             name,
		topicPath:        rootPath + separator + name,
		currentBlock:     0,
		currentBlockSize: 0,
		currentOffset:    0,
		maxBlockSize:     maxBlockSize,
	}
	topicExist := doesTopicExist(rootPath, name)
	if topicExist {
		blocksSorted, err := listBlocksSorted(topic.topicPath)
		if err != nil {
			return nil, err
		}
		if len(blocksSorted) == 0 {
			err = topic.createFirstBlock()
			if err != nil {
				return nil, err
			}
		}
		block, err := topic.findCurrentBlock(topic.topicPath)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		fileName := createBlockFileName(block)
		blockFileName := topic.topicPath + separator + fileName
		topic.logFile, err = NewLogWriter(blockFileName)
		if err != nil {
			return nil, err
		}
		topic.currentBlockSize = blockSize(topic.logFile.LogFile)
		topic.currentBlock = block
		logReader, err := NewLogReader(blockFileName)
		if err != nil {
			return nil, err
		}
		offset, err := logReader.ReadCurrentOffset()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		topic.currentOffset = offset
		err = logReader.CloseLogReader()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New(fmt.Sprintf("topic [%s] is not registered", topic.name))
	}
	return topic, nil
}

func (t *TopicWrite) Close() error {
	return t.logFile.CloseLogWriter()
}

func (t *TopicWrite) findCurrentBlock(topicPath string) (uint64, error) {
	sorted, err := listBlocksSorted(topicPath)
	if err != nil {
		return 0, err
	}
	return sorted[len(sorted)-1], nil
}

func (t *TopicWrite) createNextBlock() error {
	err := t.logFile.CloseLogWriter()
	if err != nil {
		return err
	}
	t.currentBlock = t.currentOffset
	newBlockFileName := t.topicPath + separator + createBlockFileName(t.currentBlock+1)
	t.logFile, err = NewLogWriter(newBlockFileName)
	if err != nil {
		return err
	}
	t.currentBlockSize = 0
	return nil
}

func (t *TopicWrite) createTopic() (bool, error) {
	return createTopic(t.rootPath, t.name)
}

func (t *TopicWrite) createFirstBlock() error {
	fileName := createBlockFileName(t.currentBlock)
	writer, err := NewLogWriter(t.topicPath + separator + fileName)
	if err != nil {
		return err
	}
	t.logFile = writer
	return nil
}

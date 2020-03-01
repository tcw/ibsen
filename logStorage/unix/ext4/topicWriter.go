package ext4

import (
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"os"
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
}

func (t *TopicWrite) WriteToTopic(entry []byte) int {
	t.currentOffset = t.currentOffset + 1
	logEntry := &logStorage.LogEntry{
		Offset:   logStorage.Offset(t.currentOffset),
		ByteSize: len(entry),
		Entry:    entry,
	}
	n := t.logFile.WriteToFile(logEntry)
	t.currentBlockSize = t.currentBlockSize + int64(logEntry.ByteSize) + 16 // 16 is offset + byteSize
	if t.currentBlockSize > t.maxBlockSize {
		t.createNextBlock()
	}
	return n
}

func (t *TopicWrite) findCurrentBlock(topicPath string) (uint64, error) {
	sorted, err := listBlocksSorted(topicPath)
	if err != nil {
		return 0, err
	}
	return sorted[len(sorted)-1], nil
}

func NewTopicWrite(rootpath string, name string, maxBlockSize int64) (TopicWrite, error) {

	topic := TopicWrite{
		rootPath:         rootpath,
		name:             name,
		topicPath:        rootpath + separator + name,
		currentBlock:     0,
		currentBlockSize: 0,
		currentOffset:    0,
		maxBlockSize:     maxBlockSize,
	}
	topicExist := doesTopicExist(rootpath, name)
	if topicExist {
		blocksSorted, err := listBlocksSorted(topic.topicPath)
		if err != nil {
			return TopicWrite{}, err
		}
		if len(blocksSorted) == 0 {
			err := topic.createTopic()
			if err == nil {
				topic.createFirstBlock()
			} else {
				return TopicWrite{}, err
			}
		}
	}
	if topicExist {
		block, err := topic.findCurrentBlock(topic.topicPath)
		if err != nil {
			log.Println(err)
			return TopicWrite{}, err
		}
		fileName := createBlockFileName(block)
		blockFileName := topic.topicPath + separator + fileName
		topic.logFile = NewLogWriter(blockFileName)
		topic.currentBlockSize = blockSize(topic.logFile.LogFile)
		topic.currentBlock = block
		reader := NewLogReader(blockFileName)
		offset, err := reader.ReadCurrentOffset()
		if err != nil {
			log.Println(err)
			return TopicWrite{}, err
		}
		topic.currentOffset = offset
		reader.CloseLogReader()
	} else {
		err := topic.createTopic()
		if err == nil {
			topic.createFirstBlock()
		} else {
			return TopicWrite{}, err
		}
	}
	return topic, nil
}

func (t *TopicWrite) createNextBlock() {
	t.logFile.CloseLogWriter()
	t.currentBlock = t.currentOffset
	newBlockFileName := t.topicPath + separator + createBlockFileName(t.currentBlock+1)
	t.logFile = NewLogWriter(newBlockFileName)
	t.currentBlockSize = 0
}

func createNewTopic(rootPath string, topicName string) error {
	topics, err := ListTopics(rootPath)
	if err != nil {
		return err
	}
	for _, v := range topics {
		if topicName == v {
			return errors.New("Topic exist")
		}
	}
	err = os.Mkdir(rootPath+separator+topicName, 0777) //Todo: more restrictive
	if err != nil {
		return err
	}
	return nil
}

func (t *TopicWrite) createTopic() error {
	err := os.Mkdir(t.topicPath, 0777)
	if err != nil {
		log.Println("Couldn't create topic", t.name)
		return err
	}
	return nil
}

func (t *TopicWrite) createFirstBlock() {
	fileName := createBlockFileName(t.currentBlock)
	t.logFile = NewLogWriter(t.topicPath + separator + fileName)
}

package ext4

import (
	"errors"
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

func (t *TopicWrite) WriteToTopic(payload []byte) int {
	t.currentOffset = t.currentOffset + 1
	entry := LogEntry{
		Offset:  t.currentOffset,
		Size:    uint64(len(payload)),
		Payload: payload,
	}
	n := t.logFile.WriteToFile(entry)
	t.currentBlockSize = t.currentBlockSize + int64(entry.Size) + 16
	if t.currentBlockSize > t.maxBlockSize {
		t.createNextBlock()
	}
	return n
}

func NewTopicWrite(rootpath string, name string, maxBlockSize int64) *TopicWrite {

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
		blocksSorted := listBlocksSorted(topic.topicPath)
		if len(blocksSorted) == 0 {
			createdTopic := topic.createTopic()
			if createdTopic {
				topic.createFirstBlock()
			}
		}
	}
	if topicExist {
		block, err := topic.findCurrentBlock(topic.topicPath)
		if err != nil {
			log.Println(err)
		}
		fileName := createBlockFileName(block)
		blockFileName := topic.topicPath + separator + fileName
		topic.logFile = persistentLog.NewLogWriter(blockFileName)
		topic.currentBlockSize = blockSize(topic.logFile.LogFile)
		topic.currentBlock = block
		reader := persistentLog.NewLogReader(blockFileName)
		offset, err := reader.ReadCurrentOffset()
		if err != nil {
			log.Println(err)
		}
		topic.currentOffset = offset
		reader.CloseLogReader()
	} else {
		createdTopic := topic.createTopic()
		if createdTopic {
			topic.createFirstBlock()
		}
	}
	return &topic
}

func (t *TopicWrite) createNextBlock() {
	t.logFile.CloseLogWriter()
	t.currentBlock = t.currentOffset
	newBlockFileName := t.topicPath + separator + createBlockFileName(t.currentBlock+1)
	t.logFile = persistentLog.NewLogWriter(newBlockFileName)
	t.currentBlockSize = 0
}

func CreateNewTopic(rootPath string, topicName string) error {
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

func (t *TopicWrite) createTopic() bool {
	err := os.Mkdir(t.topicPath, 0777)
	if err != nil {
		log.Println("Couldn't create topic", t.name)
		return false
	}
	return true
}

func (t *TopicWrite) createFirstBlock() {
	fileName := createBlockFileName(t.currentBlock)
	t.logFile = persistentLog.NewLogWriter(t.topicPath + separator + fileName)
}

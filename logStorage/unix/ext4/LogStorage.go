package ext4

import (
	"bufio"
	"errors"
	"github.com/tcw/ibsen/api/grpc/golangApi"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"os"
	"sync"
)

func createWriters(rootPath string, maxBlockSize int64) ([]topicWriteConfig, error) {
	topics, err := ListTopics(rootPath)
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		log.Printf("Registered topic [%s]", topic)
		writer, err := NewTopicWrite(rootPath, topic, maxBlockSize)
		if err != nil {
			return nil, err
		}
		writers[topic] = writer
	}
	return writers, nil
}

type WriteTask struct {
	entry logStorage.Entry
	done  bool
}

type topicWriteConfig struct {
	rootPath     string
	maxBlockSize int64
	topic        logStorage.Topic
	task         chan WriteTask //Todo: should this be a pointer?
	writerError  chan error
}

func registerTopicWriterJob(config chan topicWriteConfig, errChan chan error) {
	var topicWriterJobs []topicWriteConfig
}

func topicWriterJob(config topicWriteConfig) {
	topicWrite, err := NewTopicWrite(config.rootPath, string(config.topic), config.maxBlockSize)
	if err != nil {
		config.writerError <- err
		return
	}
	for {
		task := <-config.task
		if task.done {
			return
		}
		_, err = topicWrite.WriteToTopic(task.entry)
		if err != nil {
			config.writerError <- err
			return
		}
	}
}

type LogFile struct {
	LogWriter *bufio.Writer
	LogFile   *os.File
	FileName  string
}

type LogStorage struct {
	rootPath     string
	maxBlockSize int64
	topicWriters map[string]*TopicWrite
}

func NewLogStorage(rootPath string, maxBlockSize int64) (LogStorage, error) {
	storage := LogStorage{
		rootPath:     rootPath,
		maxBlockSize: maxBlockSize,
		topicWriters: nil,
	}
	writers, err := createWriters(rootPath, maxBlockSize)
	if err != nil {
		return LogStorage{}, err
	}
	storage.topicWriters = writers
	return storage, nil
}

var _ logStorage.LogStorage = LogStorage{} // Verify that T implements I.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that *T implements I.

func (e LogStorage) Create(topic *golangApi.Topic) (bool, error) {

	created, err := createTopic(e.rootPath, topic.Name)
	if err != nil {
		return false, err
	}
	if created {
		writer, err := NewTopicWrite(e.rootPath, topic.Name, e.maxBlockSize)
		if err != nil {
			return false, err
		}
		e.topicWriters[topic.Name] = writer
		log.Printf("Registered topic [%s]", topic)
		return true, nil
	}
	return false, nil
}

func (e LogStorage) Drop(topic *golangApi.Topic) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topicMessage *golangApi.TopicMessage) (int, error) {
	topicWrite := e.topicWriters[topicMessage.TopicName]
	if topicWrite == nil {
		return 0, errors.New("Topic does not exist")
	}
	n, err := topicWrite.WriteToTopic(topicMessage)
	if err != nil {
		return 0, err
	}
	return n, nil

}

func (e LogStorage) ReadFromBeginning(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic *golangApi.Topic) error {
	reader, err := NewTopicRead(e.rootPath, string(topic))
	if err != nil {
		return err
	}
	err = reader.ReadFromBeginning(logChan, wg)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadFromNotIncluding(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic *golangApi.Topic, offset *golangApi.Offset) error {
	reader, err := NewTopicRead(e.rootPath, string(topic))
	if err != nil {
		return err
	}
	err = reader.ReadLogFromOffsetNotIncluding(logChan, wg, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ListTopics() ([]logStorage.Topic, error) {
	panic("implement me")
}

func (e LogStorage) Close() {
	for _, v := range e.topicWriters {
		v.Close()
	}
}

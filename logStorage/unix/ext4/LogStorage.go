package ext4

import (
	"bufio"
	"errors"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"os"
	"sync"
)

func createWriters(rootPath string, maxBlockSize int64) (map[string]*TopicWrite, error) {
	topics, err := ListTopics(rootPath)
	writers := make(map[string]*TopicWrite)
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

func (e LogStorage) Create(topic logStorage.Topic) (bool, error) {
	sTopic := string(topic)
	created, err := createTopic(e.rootPath, sTopic)
	if err != nil {
		return false, err
	}
	if created {
		writer, err := NewTopicWrite(e.rootPath, sTopic, e.maxBlockSize)
		if err != nil {
			return false, err
		}
		e.topicWriters[sTopic] = writer
		log.Printf("Registered topic [%s]", topic)
	}
	return true, err
}

func (e LogStorage) Drop(topic logStorage.Topic) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topic logStorage.Topic, entry logStorage.Entry) (int, error) {
	topicWrite := e.topicWriters[string(topic)]
	if topicWrite == nil {
		return 0, errors.New("Topic does not exist")
	}
	n, err := topicWrite.WriteToTopic(entry)
	if err != nil {
		return 0, err
	}
	return n, nil

}

func (e LogStorage) ReadFromBeginning(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic logStorage.Topic) error {
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

func (e LogStorage) ReadFromNotIncluding(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic logStorage.Topic, offset logStorage.Offset) error {
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

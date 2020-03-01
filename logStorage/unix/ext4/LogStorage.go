package ext4

import (
	"bufio"
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
	for _, v := range topics {
		log.Println(v)
		writer, err := NewTopicWrite(rootPath, v, maxBlockSize)
		if err != nil {
			return nil, err
		}
		writers[v] = writer
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
	topicWriters map[string]*TopicWrite
}

func NewLogStorage(rootPath string, maxBlockSize int64) (LogStorage, error) {
	storage := LogStorage{
		rootPath:     rootPath,
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
	panic("implement me")
}

func (e LogStorage) Drop(topic logStorage.Topic) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topic logStorage.Topic, entry logStorage.Entry) (int, error) {

	n, err := e.topicWriters[string(topic)].WriteToTopic(entry)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (e LogStorage) ReadFromBeginning(logChan chan *logStorage.LogEntry, topic logStorage.Topic) error {
	reader, err := NewTopicRead(e.rootPath, string(topic))
	if err != nil {
		return err
	}
	var wg sync.WaitGroup //Todo: propagate wg
	err = reader.ReadFromBeginning(logChan, &wg)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadFromNotIncluding(logChan chan *logStorage.LogEntry, topic logStorage.Topic, offset logStorage.Offset) error {
	reader, err := NewTopicRead(e.rootPath, string(topic))
	if err != nil {
		return err
	}
	var wg sync.WaitGroup //Todo: propagate wg
	err = reader.ReadLogFromOffsetNotIncluding(logChan, &wg, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ListTopics() ([]logStorage.Topic, error) {
	panic("implement me")
}

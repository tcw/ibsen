package ext4

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"os"
	"sync"
)

func registerTopics(rootPath string, maxBlockSize int64) ([]*topicWriteConfig, error) {
	topics, err := ListTopics(rootPath)
	var topicWriters []*topicWriteConfig
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		config, err := registerTopic(rootPath, topic, maxBlockSize)
		if err != nil {
			return nil, err
		}
		topicWriters = append(topicWriters, config)
	}
	return topicWriters, nil
}

func registerTopic(rootPath string, topic string, maxBlockSize int64) (*topicWriteConfig, error) {
	config := &topicWriteConfig{
		rootPath:     rootPath,
		maxBlockSize: maxBlockSize,
		topic:        topic,
		task:         make(chan *WriteTask),
		writerError:  make(chan error),
	}
	go runTopicWriterJob(config)
	return config, nil
}

type WriteTask struct {
	entry *[]byte
	done  bool
}

type topicWriteConfig struct {
	rootPath     string
	maxBlockSize int64
	topic        string
	task         chan *WriteTask
	writerError  chan error
	wg           sync.WaitGroup
}

func runTopicWriterJob(config *topicWriteConfig) {
	topicWrite, err := NewTopicWrite(config.rootPath, config.topic, config.maxBlockSize)
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

		} else {
			config.writerError <- nil
		}
		config.wg.Done()
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
	wgTopic      sync.WaitGroup
	topicWriters map[string]*topicWriteConfig
	mu           sync.Mutex
}

func NewLogStorage(rootPath string, maxBlockSize int64) (*LogStorage, error) {
	storage := &LogStorage{
		rootPath:     rootPath,
		maxBlockSize: maxBlockSize,
		topicWriters: make(map[string]*topicWriteConfig),
	}

	storage.mu.Lock()

	topics, err := registerTopics(rootPath, maxBlockSize)
	for _, v := range topics {
		storage.topicWriters[v.topic] = v
	}
	if err != nil {
		log.Fatal("Unable to register topics")
	}
	storage.mu.Unlock()
	return storage, nil
}

var _ logStorage.LogStorage = LogStorage{} // Verify that T implements I.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that *T implements I.

func (e LogStorage) Create(topic string) (bool, error) {
	e.mu.Lock()

	_, exists := e.topicWriters[topic]
	if exists {
		return false, nil
	}

	_, err2 := createTopic(e.rootPath, topic)
	if err2 != nil {
		return false, err2
	}
	config, err := registerTopic(e.rootPath, topic, e.maxBlockSize)
	if err != nil {
		return false, err
	}
	e.topicWriters[topic] = config
	e.mu.Unlock()
	return true, nil
}

func (e LogStorage) Drop(topic string) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topicMessage logStorage.TopicMessage) (int, error) {
	writeConfig := e.topicWriters[topicMessage.Topic]
	if writeConfig == nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s], topic not registered", topicMessage.Topic))
	}
	writeConfig.wg.Add(1)
	writeConfig.task <- &WriteTask{
		entry: topicMessage.Message,
		done:  false,
	}
	if <-writeConfig.writerError != nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s]", topicMessage.Topic))
	}
	writeConfig.wg.Wait()
	return 1, nil
}

func (e LogStorage) ReadFromBeginning(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic string) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadFromBeginning(logChan, wg)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ReadFromNotIncluding(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, topic string, offset uint64) error {
	reader, err := NewTopicRead(e.rootPath, topic)
	if err != nil {
		return err
	}
	err = reader.ReadLogFromOffsetNotIncluding(logChan, wg, offset)
	if err != nil {
		return err
	}
	return nil
}

func (e LogStorage) ListTopics() ([]string, error) {
	panic("implement me")
}

func (e LogStorage) Close() {
	for _, v := range e.topicWriters {
		v.task <- &WriteTask{
			entry: nil,
			done:  true,
		}
	}
}

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

func registerTopics(rootPath string, maxBlockSize int64, registerChan chan *topicWriteConfig, wg *sync.WaitGroup) ([]*topicWriteConfig, error) {
	topics, err := ListTopics(rootPath)
	var topicWriters []*topicWriteConfig
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		config, err := registerTopic(rootPath, topic, maxBlockSize, registerChan, wg)
		if err != nil {
			return nil, err
		}
		topicWriters = append(topicWriters, config)
	}
	return topicWriters, nil
}

func registerTopic(rootPath string, topic string, maxBlockSize int64, registerChan chan *topicWriteConfig, wg *sync.WaitGroup) (*topicWriteConfig, error) {
	config := &topicWriteConfig{
		rootPath:     rootPath,
		maxBlockSize: maxBlockSize,
		topic:        topic,
		task:         make(chan *WriteTask),
		writerError:  make(chan error),
	}
	wg.Add(1)
	registerChan <- config
	wg.Done()
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
}

func registerTopicWriterJob(config chan *topicWriteConfig, wg *sync.WaitGroup) {
	var topicWriterJobs []topicWriteConfig
	for {
		writeConfig := <-config

		jobExists := false
		for _, topicJob := range topicWriterJobs {
			if topicJob.topic == writeConfig.topic {
				log.Printf("Tried to register already registered topic %s", writeConfig.topic)
				jobExists = true
				break
			}
		}
		if !jobExists {
			topicWriterJobs = append(topicWriterJobs, *writeConfig)
			go runTopicWriterJob(*writeConfig)
		}
		wg.Done()
	}
}

func runTopicWriterJob(config topicWriteConfig) {
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
	rootPath           string
	maxBlockSize       int64
	wgTopic            *sync.WaitGroup
	chanWriterRegister chan *topicWriteConfig
	topicWriters       map[string]*topicWriteConfig
	mu                 sync.Mutex
}

func NewLogStorage(rootPath string, maxBlockSize int64) (*LogStorage, error) {
	storage := &LogStorage{
		rootPath:           rootPath,
		maxBlockSize:       maxBlockSize,
		chanWriterRegister: make(chan *topicWriteConfig),
		topicWriters:       make(map[string]*topicWriteConfig),
	}
	storage.mu.Lock()
	go registerTopicWriterJob(storage.chanWriterRegister, storage.wgTopic)
	topics, err := registerTopics(rootPath, maxBlockSize, storage.chanWriterRegister, storage.wgTopic)
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
	config, err := registerTopic(e.rootPath, topic, e.maxBlockSize, e.chanWriterRegister, e.wgTopic)
	if err != nil {
		return false, err
	}
	e.topicWriters[topic] = config
	e.mu.Unlock()
	return true, nil // Todo: Is this behaviour really wanted?
}

func (e LogStorage) Drop(topic string) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topicMessage logStorage.TopicMessage) (int, error) {
	writeConfig := e.topicWriters[topicMessage.Topic]
	if writeConfig == nil {
		return 0, errors.New(fmt.Sprintf("Error writing to topic [%s]", topicMessage.Topic))
	}
	writeConfig.task <- &WriteTask{
		entry: topicMessage.Message,
		done:  false,
	}
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

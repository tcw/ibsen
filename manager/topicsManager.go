package manager

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
	"sync"
	"time"
)

type ReadParams struct {
	TopicName common.TopicName
	LogChan   chan *[]common.LogEntry
	Wg        *sync.WaitGroup
	From      common.Offset
	BatchSize uint32
	// Cancel stops the read when closed; nil never cancels.
	Cancel <-chan struct{}
}

type LogManager interface {
	List() []common.TopicName
	Write(topic common.TopicName, entries common.EntriesPtr) error
	Read(params ReadParams) error
}

var _ LogManager = &LogTopicsManager{}

type LogTopicManagerParams struct {
	ReadOnly         bool
	Afs              *afero.Afero
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSize     int
	RootPath         string
}

type LogTopicsManager struct {
	Params           LogTopicManagerParams
	TopicWriteLocker *sync.Map
	Topics           *sync.Map
	StatusAccess     access.StatusAccess
	// loads holds a *topicLoad for each topic whose first load is running
	loads *sync.Map
	state *managerState
}

// ErrClosed is returned by writes, and by reads of topics that are not loaded, after Close.
var ErrClosed = errors.New("log manager is closed")

// managerState tracks requests that can write to storage, so Close can wait for them.
type managerState struct {
	mu          sync.RWMutex
	closed      bool
	requests    sync.WaitGroup
	stopIndexer chan struct{}
	indexerDone chan struct{}
	stopOnce    sync.Once
}

// topicLoad is the first load of a topic from disk, shared by every request for the topic
// that arrives while it runs.
type topicLoad struct {
	done  chan struct{}
	topic *access.Topic
	err   error
}

var TopicNotFound = errors.New("topic not found")

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	manager := LogTopicsManager{
		Params:           params,
		TopicWriteLocker: &sync.Map{},
		Topics:           &sync.Map{},
		loads:            &sync.Map{},
		state: &managerState{
			stopIndexer: make(chan struct{}),
			indexerDone: make(chan struct{}),
		},
		StatusAccess: &access.Status{
			Afs:      params.Afs,
			RootPath: params.RootPath,
		},
	}
	go manager.startIndexScheduler()
	return manager, nil
}

// Close stops accepting writes and topic loads, waits for those in flight, stops the index
// scheduler and waits for background indexing, so nothing writes to storage once it
// returns. Reads of loaded topics are not waited for: they do not write, and a tailing read
// lasts until its client leaves. Calling Close again is a no-op.
func (l *LogTopicsManager) Close() {
	l.state.mu.Lock()
	l.state.closed = true
	l.state.mu.Unlock()
	l.state.requests.Wait()
	l.state.stopOnce.Do(func() { close(l.state.stopIndexer) })
	<-l.state.indexerDone
	l.Topics.Range(func(_, topic any) bool {
		topic.(*access.Topic).Close()
		return true
	})
}

// begin registers a request that may write to storage, or fails once the manager is closed.
// Call l.state.requests.Done when it finishes.
func (l *LogTopicsManager) begin() error {
	l.state.mu.RLock()
	defer l.state.mu.RUnlock()
	if l.state.closed {
		return ErrClosed
	}
	l.state.requests.Add(1)
	return nil
}

func (l *LogTopicsManager) List() []common.TopicName {
	return l.StatusAccess.List()
}

func (l *LogTopicsManager) Write(topicName common.TopicName, entries common.EntriesPtr) error {
	if l.Params.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.state.requests.Done()
	topic, err := l.getOrCreateTopic(topicName)
	if err != nil {
		return err
	}
	locker, _ := l.TopicWriteLocker.LoadOrStore(string(topicName), &sync.Mutex{})
	var mutex = locker.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	return topic.Write(entries)
}

func (l *LogTopicsManager) Read(params ReadParams) error {
	topic, err := l.loadForRead(params.TopicName)
	if err != nil {
		return err
	}
	readFrom := params.From
	return topic.Read(common.ReadLogParams{
		LogChan:   params.LogChan,
		Wg:        params.Wg,
		From:      readFrom,
		BatchSize: params.BatchSize,
		Cancel:    params.Cancel,
	})
}

// loadForRead returns the topic to read. A loaded topic stays readable after Close; only a
// load is tracked as in flight, since loading recovers the head block and may write.
func (l *LogTopicsManager) loadForRead(name common.TopicName) (*access.Topic, error) {
	if topic, ok := l.Topics.Load(string(name)); ok {
		return topic.(*access.Topic), nil
	}
	if err := l.begin(); err != nil {
		return nil, err
	}
	defer l.state.requests.Done()
	return l.getOrCreateTopic(name)
}

// getOrCreateTopic returns the cached topic, loading it first if needed. A topic is loaded
// at most once at a time: loading recovers the head block and rewrites its index, so a
// second load running beside a topic that already accepts writes could truncate
// acknowledged entries or clobber index pairs. Requests that arrive during a load wait for it.
func (l *LogTopicsManager) getOrCreateTopic(name common.TopicName) (*access.Topic, error) {
	if topic, ok := l.Topics.Load(string(name)); ok {
		return topic.(*access.Topic), nil
	}
	load := &topicLoad{done: make(chan struct{})}
	if running, isRunning := l.loads.LoadOrStore(string(name), load); isRunning {
		load = running.(*topicLoad)
		<-load.done
		return load.topic, load.err
	}
	// a load that finished after the check above has already cached the topic, because a
	// load is removed from loads only after its topic is stored
	if topic, ok := l.Topics.Load(string(name)); ok {
		load.topic = topic.(*access.Topic)
	} else {
		load.topic, load.err = l.loadOrCreateNewTopic(name)
		if load.err == nil {
			l.Topics.Store(string(name), load.topic)
		}
	}
	l.loads.Delete(string(name))
	close(load.done)
	return load.topic, load.err
}

// loadOrCreateNewTopic loads a topic from disk or creates it. A topic that fails to load is
// not cached, so its requests keep failing until the problem is fixed while other topics
// keep working.
func (l *LogTopicsManager) loadOrCreateNewTopic(topicName common.TopicName) (*access.Topic, error) {
	topic := access.NewLogTopic(common.TopicParams{
		Afs:          l.Params.Afs,
		RootPath:     l.Params.RootPath,
		TopicName:    string(topicName),
		MaxBlockSize: l.Params.MaxBlockSize,
	})
	if err := topic.LoadOrCreate(); err != nil {
		return nil, errore.WrapWithContextF(err, "unable to load topic %s", topicName)
	}
	return topic, nil
}

func (l *LogTopicsManager) startIndexScheduler() {
	defer close(l.state.indexerDone)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.state.stopIndexer:
			return
		case <-ticker.C:
			l.Topics.Range(func(key, value any) bool {
				_, err := value.(*access.Topic).UpdateIndex()
				if err != nil {
					log.Err(err).Msg(fmt.Sprintf("index builder for topic %s has failed", key.(string)))
				}
				return true
			})
		}
	}
}

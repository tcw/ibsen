package manager

import (
	"errors"
	"sync"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
	"github.com/tcw/ibsen/errore"
)

var _ driver.LogManager = &LogTopicsManager{}

type LogTopicManagerParams struct {
	ReadOnly         bool
	Store            driven.BlockStore
	TTL              time.Duration
	CheckForNewEvery time.Duration
	MaxBlockSize     int
	// IndexSparsity is the number of entries between two index pairs; zero means
	// topic.DefaultIndexSparsity.
	IndexSparsity uint32
	// Codec compresses the frames written from now on; nil writes them uncompressed.
	Codec driven.Codec
	// Codecs resolves the codec byte of frames already written; nil reads uncompressed
	// frames and reports anything else as an unknown codec.
	Codecs driven.Codecs
	// FlushEntries is how many entries may wait before a flush is forced; zero means
	// topic.DefaultFlushEntries, which flushes every write before acknowledging it.
	FlushEntries uint32
	// FlushInterval is how long a batch may be held back hoping for more entries; zero
	// never holds one back.
	FlushInterval time.Duration
	// Logger is optional: a core built without one logs nothing rather than crashing.
	Logger driven.Logger
}

type LogTopicsManager struct {
	Params           LogTopicManagerParams
	TopicWriteLocker *sync.Map
	Topics           *sync.Map
	StatusAccess     topic.StatusAccess
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
	topic *topic.Topic
	err   error
}

var TopicNotFound = errors.New("topic not found")

func NewLogTopicsManager(params LogTopicManagerParams) (LogTopicsManager, error) {
	if params.Logger == nil {
		params.Logger = driven.NopLogger{}
	}
	manager := LogTopicsManager{
		Params:           params,
		TopicWriteLocker: &sync.Map{},
		Topics:           &sync.Map{},
		loads:            &sync.Map{},
		state: &managerState{
			stopIndexer: make(chan struct{}),
			indexerDone: make(chan struct{}),
		},
		StatusAccess: &topic.Status{Store: params.Store, Log: params.Logger},
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
	l.Topics.Range(func(_, loaded any) bool {
		loaded.(*topic.Topic).Close()
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

func (l *LogTopicsManager) List() []domain.TopicName {
	return l.StatusAccess.List()
}

func (l *LogTopicsManager) Write(topicName domain.TopicName, entries domain.EntriesPtr) error {
	if l.Params.ReadOnly {
		return errors.New("ibsen is in read only mode and will not accept any writes")
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.state.requests.Done()
	loaded, err := l.getOrCreateTopic(topicName)
	if err != nil {
		return err
	}
	locker, _ := l.TopicWriteLocker.LoadOrStore(string(topicName), &sync.Mutex{})
	var mutex = locker.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	return loaded.Write(entries)
}

func (l *LogTopicsManager) Read(params driver.ReadParams) error {
	loaded, err := l.loadForRead(params.TopicName)
	if err != nil {
		return err
	}
	readFrom := params.From
	return loaded.Read(domain.ReadLogParams{
		LogChan:   params.LogChan,
		Wg:        params.Wg,
		From:      readFrom,
		BatchSize: params.BatchSize,
		Cancel:    params.Cancel,
	})
}

// loadForRead returns the topic to read. A loaded topic stays readable after Close; only a
// load is tracked as in flight, since loading recovers the head block and may write.
func (l *LogTopicsManager) loadForRead(name domain.TopicName) (*topic.Topic, error) {
	if loaded, ok := l.Topics.Load(string(name)); ok {
		return loaded.(*topic.Topic), nil
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
func (l *LogTopicsManager) getOrCreateTopic(name domain.TopicName) (*topic.Topic, error) {
	if loaded, ok := l.Topics.Load(string(name)); ok {
		return loaded.(*topic.Topic), nil
	}
	load := &topicLoad{done: make(chan struct{})}
	if running, isRunning := l.loads.LoadOrStore(string(name), load); isRunning {
		load = running.(*topicLoad)
		<-load.done
		return load.topic, load.err
	}
	// a load that finished after the check above has already cached the topic, because a
	// load is removed from loads only after its topic is stored
	if loaded, ok := l.Topics.Load(string(name)); ok {
		load.topic = loaded.(*topic.Topic)
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
func (l *LogTopicsManager) loadOrCreateNewTopic(topicName domain.TopicName) (*topic.Topic, error) {
	loaded := topic.NewLogTopic(topic.Params{
		Logger:        l.Params.Logger,
		IndexSparsity: l.Params.IndexSparsity,
		Codec:         l.Params.Codec,
		Codecs:        l.Params.Codecs,
		FlushEntries:  l.Params.FlushEntries,
		FlushInterval: l.Params.FlushInterval,
		Store:         l.Params.Store,
		TopicName:     string(topicName),
		MaxBlockSize:  l.Params.MaxBlockSize,
	})
	if err := loaded.LoadOrCreate(); err != nil {
		return nil, errore.WrapWithContextF(err, "unable to load topic %s", topicName)
	}
	l.warnAboutStrayFiles(topicName)
	return loaded, nil
}

// strayFileLister is the optional capability of a store to report what it ignored in a
// topic. The core has no opinion about stray files; an operator does.
type strayFileLister interface {
	StrayFiles(topic domain.TopicName) ([]string, error)
}

func (l *LogTopicsManager) warnAboutStrayFiles(topicName domain.TopicName) {
	lister, ok := l.Params.Store.(strayFileLister)
	if !ok {
		return
	}
	stray, err := lister.StrayFiles(topicName)
	if err != nil {
		l.Params.Logger.Log(driven.LevelWarn, "unable to check the topic for stray files",
			driven.Err(err), driven.Str("topic", string(topicName)))
		return
	}
	for _, name := range stray {
		l.Params.Logger.Log(driven.LevelWarn, "ignoring file that is not a log or index block",
			driven.Str("topic", string(topicName)), driven.Str("file", name))
	}
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
				_, err := value.(*topic.Topic).UpdateIndex()
				if err != nil {
					l.Params.Logger.Log(driven.LevelError, "index builder for topic has failed",
						driven.Err(err), driven.Str("topic", key.(string)))
				}
				return true
			})
		}
	}
}

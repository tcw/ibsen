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
var _ driver.Flusher = &LogTopicsManager{}

type LogTopicManagerParams struct {
	ReadOnly bool
	Store    driven.BlockStore
	// TTL is unused here; the gRPC adapter carries its own connection TTL.
	TTL          time.Duration
	MaxBlockSize int
	// IndexSparsity is the number of entries between two index pairs; zero means
	// topic.DefaultIndexSparsity.
	IndexSparsity uint32
	// MaxFrameEntries and MaxFrameBytes bound one frame; zero means the topic defaults.
	MaxFrameEntries uint32
	MaxFrameBytes   int
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
	// ProducerWindowBlocks is how many of a topic's newest blocks a producer is remembered
	// across; zero means topic.DefaultProducerWindowBlocks.
	ProducerWindowBlocks int
	// FlushMode says when a write is acknowledged; the zero value is topic.FlushStrict. In
	// topic.FlushLenient nothing is flushed until Flush is called, which is the composition
	// root's job: the core starts no timer.
	FlushMode topic.FlushMode
	// Logger is optional: a core built without one logs nothing rather than crashing.
	Logger driven.Logger
}

type LogTopicsManager struct {
	Params       LogTopicManagerParams
	Topics       *sync.Map
	StatusAccess topic.StatusAccess
	// loads holds a *topicLoad for each topic whose first load is running
	loads *sync.Map
	state *managerState
}

// ErrClosed is returned by writes, and by reads of topics that are not loaded, after Close.
var ErrClosed = errors.New("log manager is closed")

// managerState tracks requests that can write to storage, so Close can wait for them.
type managerState struct {
	mu       sync.RWMutex
	closed   bool
	requests sync.WaitGroup
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
		Params:       params,
		Topics:       &sync.Map{},
		loads:        &sync.Map{},
		state:        &managerState{},
		StatusAccess: &topic.Status{Store: params.Store, Log: params.Logger},
	}
	return manager, nil
}

// Close stops accepting writes and topic loads, waits for those in flight, flushes every
// topic, and waits for the background indexing each topic's writes started, so nothing writes
// to storage once it returns. It returns the first flush error. Reads of loaded topics are
// not waited for: they do not write, and a tailing read lasts until its client leaves.
// Calling Close again flushes nothing new.
func (l *LogTopicsManager) Close() error {
	l.state.mu.Lock()
	l.state.closed = true
	l.state.mu.Unlock()
	l.state.requests.Wait()
	var first error
	l.Topics.Range(func(_, loaded any) bool {
		if err := loaded.(*topic.Topic).Close(); err != nil && first == nil {
			first = err
		}
		return true
	})
	return first
}

// Retain applies retention to every topic in the store, loading those not loaded yet, and
// returns the first error; a topic that fails does not stop the others. policy says what to
// keep of each topic. now is the caller's clock, since the core reads none. A read-only manager
// deletes nothing.
func (l *LogTopicsManager) Retain(policy func(domain.TopicName) topic.RetentionPolicy, now time.Time) error {
	if l.Params.ReadOnly {
		return nil
	}
	if err := l.begin(); err != nil {
		return err
	}
	defer l.state.requests.Done()
	var first error
	for _, name := range l.List() {
		chosen := policy(name)
		if chosen.Keeps() {
			continue
		}
		loaded, err := l.getOrCreateTopic(name)
		if err == nil {
			_, err = loaded.Retain(chosen, now)
		}
		if err != nil && first == nil {
			first = errore.WrapWithContextF(err, "unable to apply retention to topic %s", name)
		}
	}
	return first
}

// Flush makes every entry appended to any loaded topic before the call durable, and so
// readable, and returns the first error. Every topic is flushed even when one fails, since a
// failure stops only its own topic.
func (l *LogTopicsManager) Flush() error {
	var first error
	l.Topics.Range(func(name, loaded any) bool {
		if err := loaded.(*topic.Topic).Flush(); err != nil && first == nil {
			first = errore.WrapWithContextF(err, "unable to flush topic %s", name)
		}
		return true
	})
	return first
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

func (l *LogTopicsManager) Write(params driver.WriteParams) (domain.WriteResult, error) {
	if l.Params.ReadOnly {
		return domain.WriteResult{}, errors.New("ibsen is in read only mode and will not accept any writes")
	}
	if err := l.begin(); err != nil {
		return domain.WriteResult{}, err
	}
	defer l.state.requests.Done()
	loaded, err := l.getOrCreateTopic(params.Topic)
	if err != nil {
		return domain.WriteResult{}, err
	}
	// no lock of our own: the topic assigns offsets under its own, and writers to one topic
	// have to be inside it together for their entries to share a flush
	return loaded.WriteWith(topic.WriteRequest{Entries: params.Entries, Producer: params.Producer, Sequence: params.Sequence})
}

func (l *LogTopicsManager) Read(params driver.ReadParams) error {
	loaded, err := l.loadForRead(params.TopicName)
	if err != nil {
		return err
	}
	readFrom := params.From
	return loaded.Read(domain.ReadLogParams{
		LogChan:      params.LogChan,
		Wg:           params.Wg,
		From:         readFrom,
		BatchSize:    params.BatchSize,
		Cancel:       params.Cancel,
		FromEarliest: params.FromEarliest,
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
		Logger:               l.Params.Logger,
		IndexSparsity:        l.Params.IndexSparsity,
		MaxFrameEntries:      l.Params.MaxFrameEntries,
		MaxFrameBytes:        l.Params.MaxFrameBytes,
		Codec:                l.Params.Codec,
		Codecs:               l.Params.Codecs,
		FlushEntries:         l.Params.FlushEntries,
		FlushInterval:        l.Params.FlushInterval,
		FlushMode:            l.Params.FlushMode,
		ProducerWindowBlocks: l.Params.ProducerWindowBlocks,
		Store:                l.Params.Store,
		TopicName:            string(topicName),
		MaxBlockSize:         l.Params.MaxBlockSize,
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

package topic

import (
	"sync"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/port/driven"
)

// recordingLogger is a driven.Logger that keeps what the core sent it.
type recordingLogger struct {
	mu       sync.Mutex
	minLevel driven.Level
	events   []event
}

type event struct {
	level  driven.Level
	msg    string
	fields []driven.Field
}

func (r *recordingLogger) Enabled(level driven.Level) bool { return level >= r.minLevel }

func (r *recordingLogger) Log(level driven.Level, msg string, fields ...driven.Field) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event{level: level, msg: msg, fields: fields})
}

func (r *recordingLogger) find(msg string) (event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.msg == msg {
			return e, true
		}
	}
	return event{}, false
}

func (r *recordingLogger) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func field(e event, key string) (driven.Field, bool) {
	for _, f := range e.fields {
		if f.Key == key {
			return f, true
		}
	}
	return driven.Field{}, false
}

// loadWrittenTopic reloads a topic that already holds entries, which is the path that
// reports what it found.
func loadWrittenTopic(t *testing.T, logger driven.Logger) *Topic {
	t.Helper()
	store := memstore.New()
	written := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1000})
	if err := written.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	entries := [][]byte{[]byte("one"), []byte("two")}
	if err := written.Write(&entries); err != nil {
		t.Fatal(err)
	}
	reloaded := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1000, Logger: logger})
	if err := reloaded.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return reloaded
}

// The core must reach its logger only through the injected port, so whatever it says shows
// up here and nowhere else.
func TestTopicLogsThroughThePort(t *testing.T) {
	logger := &recordingLogger{minLevel: driven.LevelTrace}
	loadWrittenTopic(t, logger)

	loaded, found := logger.find("loaded topic")
	if !found {
		t.Fatalf("the load was not reported through the port, got %d events", logger.count())
	}
	if loaded.level != driven.LevelDebug {
		t.Errorf("load logged at %v, want debug", loaded.level)
	}
	name, ok := field(loaded, "topic")
	if !ok || name.Kind != driven.KindString || name.Str != "t" {
		t.Errorf("load event did not carry the topic name as a string field: %+v", loaded.fields)
	}
}

// The core asks Enabled before building a debug payload; a logger that says no must not be
// handed the event anyway.
func TestDisabledLevelSuppressesTheEvent(t *testing.T) {
	logger := &recordingLogger{minLevel: driven.LevelWarn}
	loadWrittenTopic(t, logger)
	if _, found := logger.find("loaded topic"); found {
		t.Error("a debug event was emitted to a logger whose threshold is warn")
	}
}

// An embedded build wires no logging adapter at all.
func TestTopicWithoutALoggerWorks(t *testing.T) {
	topic := NewLogTopic(Params{Store: memstore.New(), TopicName: "t", MaxBlockSize: 1000})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	entries := [][]byte{[]byte("one"), []byte("two")}
	if err := topic.Write(&entries); err != nil {
		t.Fatal(err)
	}
	if topic.NextOffset != 2 {
		t.Errorf("NextOffset = %d, want 2", topic.NextOffset)
	}
}

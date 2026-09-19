package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
)

// testRoot is a real data directory, with the handful of filesystem calls these tests make
// against it. Paths are relative to the directory itself.
type testRoot struct {
	t    *testing.T
	path string
}

func newTestRoot(t *testing.T) *testRoot {
	t.Helper()
	return &testRoot{t: t, path: t.TempDir()}
}

func (r *testRoot) join(name string) string { return filepath.Join(r.path, name) }

// WriteFile creates the parent directories first, which the emulated filesystem these tests
// used to run on did implicitly and a real one does not.
func (r *testRoot) WriteFile(name string, content []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(r.join(name)), 0744); err != nil {
		return err
	}
	return os.WriteFile(r.join(name), content, perm)
}

func (r *testRoot) ReadFile(name string) ([]byte, error) { return os.ReadFile(r.join(name)) }

func (r *testRoot) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(r.join(name)) }

func (r *testRoot) Remove(name string) error { return os.Remove(r.join(name)) }

func newTestManager(t *testing.T, root *testRoot) *LogTopicsManager {
	t.Helper()
	return newTestManagerWithStore(t, filestore.NewOS(root.path))
}

func newTestManagerWithStore(t *testing.T, store driven.BlockStore) *LogTopicsManager {
	t.Helper()
	m, err := NewLogTopicsManager(LogTopicManagerParams{
		Store:        store,
		MaxBlockSize: 1000,
		TTL:          time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// stop the background indexing before the test's directory goes away: on a real
	// filesystem an indexer still writing races the cleanup, which the emulated one hid
	t.Cleanup(m.Close)
	return &m
}

// logBlockBytes builds the bytes of a log block holding count entries from an offset, each
// in a frame of its own, which is what a run of single-entry writes leaves behind.
func logBlockBytes(t *testing.T, topic string, from, count int) []byte {
	t.Helper()
	var block []byte
	for i := 0; i < count; i++ {
		offset := domain.Offset(from + i)
		entry := domain.CreateByteEntry([]byte(fmt.Sprintf("%s-%d", topic, from+i)), offset)
		frame, err := logfmt.EncodeFrame(driven.NoCodec{}, offset, 1, entry)
		if err != nil {
			t.Fatal(err)
		}
		block = append(block, frame...)
	}
	return block
}

func writeTopic(t *testing.T, m *LogTopicsManager, topic string, from, count int) {
	t.Helper()
	entries := make([][]byte, count)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf("%s-%d", topic, from+i))
	}
	if err := m.Write(domain.TopicName(topic), &entries); err != nil {
		t.Fatal(err)
	}
}

func readTopic(m *LogTopicsManager, topic string) ([]domain.LogEntry, error) {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var got []domain.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got = append(got, *batch...)
			wg.Done()
		}
		close(done)
	}()
	err := m.Read(driver.ReadParams{TopicName: domain.TopicName(topic), LogChan: logChan, Wg: &wg, BatchSize: 100})
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

func TestManager_loadsTopicWithStrayFiles(t *testing.T) {
	afs := newTestRoot(t)
	// a topic as a previous run left it, written directly so no background indexing is still
	// running when the manager loads it: one log block plus files that are not blocks
	block := logBlockBytes(t, "topic", 0, 30)
	files := map[string][]byte{
		"topic/00000000000000000000.log": block,
		"topic/README":                   []byte("not a block"),
		"topic/.DS_Store":                []byte("not a block"),
		"topic/backup.tar":               []byte("not a block"),
		"topic/123.log":                  []byte("not a block"),
		"notes.txt":                      []byte("not a topic"),
	}
	for name, content := range files {
		if err := afs.WriteFile(name, content, 0600); err != nil {
			t.Fatal(err)
		}
	}

	m := newTestManager(t, afs)
	if topics := m.List(); len(topics) != 1 || topics[0] != "topic" {
		t.Fatalf("List()=%v, want [topic]", topics)
	}
	got, err := readTopic(m, "topic")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 30 {
		t.Fatalf("read %d entries, want 30", len(got))
	}
	writeTopic(t, m, "topic", 30, 10)
	if got, err = readTopic(m, "topic"); err != nil || len(got) != 40 {
		t.Fatalf("read %d entries with err=%v, want 40", len(got), err)
	}
}

func TestManager_topicThatFailsToLoadReturnsError(t *testing.T) {
	afs := newTestRoot(t)
	// a regular file where a topic directory should be cannot be loaded as a topic
	if err := afs.WriteFile("notes.txt", []byte("not a topic"), 0600); err != nil {
		t.Fatal(err)
	}
	m := newTestManager(t, afs)
	entries := [][]byte{[]byte("x")}
	if err := m.Write("notes.txt", &entries); err == nil {
		t.Fatal("write to a topic that cannot be loaded succeeded")
	}
	if _, err := readTopic(m, "notes.txt"); err == nil {
		t.Fatal("read of a topic that cannot be loaded succeeded")
	}

	// other topics keep working
	writeTopic(t, m, "topic", 0, 3)
	if got, err := readTopic(m, "topic"); err != nil || len(got) != 3 {
		t.Fatalf("read %d entries with err=%v, want 3", len(got), err)
	}
}

// The frame bounds are a knob on a topic, and the manager is what builds topics, so a
// deployment that sets them has to see them arrive there.
func TestManager_frameBoundsReachTheTopic(t *testing.T) {
	afs := newTestRoot(t)
	m, err := NewLogTopicsManager(LogTopicManagerParams{
		Store:           filestore.NewOS(afs.path),
		TTL:             time.Minute,
		MaxBlockSize:    1 << 20,
		MaxFrameEntries: 7,
		MaxFrameBytes:   4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	writeTopic(t, &m, "topic", 0, 3)

	loaded, ok := m.Topics.Load("topic")
	if !ok {
		t.Fatal("the manager did not cache the topic it wrote to")
	}
	tp := loaded.(*topic.Topic)
	if tp.MaxFrameEntries != 7 {
		t.Errorf("topic holds MaxFrameEntries %d, want the 7 the manager was given", tp.MaxFrameEntries)
	}
	if tp.MaxFrameBytes != 4096 {
		t.Errorf("topic holds MaxFrameBytes %d, want the 4096 the manager was given", tp.MaxFrameBytes)
	}
}

// Zero means the topic defaults, the way every other knob the manager passes through works.
func TestManager_zeroFrameBoundsMeanTheTopicDefaults(t *testing.T) {
	m := newTestManager(t, newTestRoot(t))

	writeTopic(t, m, "topic", 0, 3)

	loaded, _ := m.Topics.Load("topic")
	tp := loaded.(*topic.Topic)
	if tp.MaxFrameEntries != topic.DefaultMaxFrameEntries {
		t.Errorf("topic holds MaxFrameEntries %d, want the default %d", tp.MaxFrameEntries, topic.DefaultMaxFrameEntries)
	}
	if tp.MaxFrameBytes != topic.DefaultMaxFrameBytes {
		t.Errorf("topic holds MaxFrameBytes %d, want the default %d", tp.MaxFrameBytes, topic.DefaultMaxFrameBytes)
	}
}

// Indexing is driven by writes, not by a clock. The manager used to sweep every loaded topic
// every ten seconds to catch work its own exclusion flag had dropped; the topic now takes
// that work itself, so there is nothing to sweep and nothing to wake the CPU on a device that
// would rather be asleep.
func TestManager_startsNoBackgroundGoroutine(t *testing.T) {
	store := filestore.NewOS(newTestRoot(t).path)

	before := runtime.NumGoroutine()
	m, err := NewLogTopicsManager(LogTopicManagerParams{Store: store, MaxBlockSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	after := runtime.NumGoroutine()
	m.Close()

	if after > before {
		t.Errorf("building a manager started %d goroutines, want none: indexing follows writes, not a timer",
			after-before)
	}
}

// And the index is still complete once writes stop, which is what the sweep was there for.
func TestManager_indexIsCompleteOnceWritesStop(t *testing.T) {
	afs := newTestRoot(t)
	m := newTestManager(t, afs)

	for i := 0; i < 30; i++ {
		writeTopic(t, m, "topic", i, 1)
	}
	m.Close()

	// one entry to a write is one entry to a frame, so the frames covering offsets 0, 10 and
	// 20 each earn a pair. They are spread over several blocks, since 30 frames do not fit in
	// this manager's block size, so the count is taken across all of them.
	pairs := 0
	entries, err := afs.ReadDir("topic")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".idx" {
			continue
		}
		idx, err := afs.ReadFile(filepath.Join("topic", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if len(idx)%index.PairSize != 0 {
			t.Fatalf("%s holds %d bytes, not whole pairs", entry.Name(), len(idx))
		}
		pairs = pairs + len(idx)/index.PairSize
	}
	if pairs != 3 {
		t.Errorf("the index holds %d pairs with no sweep to finish it, want 3", pairs)
	}
}

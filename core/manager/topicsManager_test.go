package manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driven/blockstore/aferostore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
)

func newTestAfs(t *testing.T) *afero.Afero {
	t.Helper()
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("data", 0744); err != nil {
		t.Fatal(err)
	}
	return afs
}

func newTestManager(t *testing.T, afs *afero.Afero) *LogTopicsManager {
	t.Helper()
	return newTestManagerWithStore(t, aferostore.New(afs, "data"))
}

func newTestManagerWithStore(t *testing.T, store driven.BlockStore) *LogTopicsManager {
	t.Helper()
	m, err := NewLogTopicsManager(LogTopicManagerParams{
		Store:            store,
		MaxBlockSize:     1000,
		TTL:              time.Second,
		CheckForNewEvery: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
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
	afs := newTestAfs(t)
	// a topic as a previous run left it, written directly so no background indexing is still
	// running when the manager loads it: one log block plus files that are not blocks
	block := logBlockBytes(t, "topic", 0, 30)
	files := map[string][]byte{
		"data/topic/00000000000000000000.log": block,
		"data/topic/README":                   []byte("not a block"),
		"data/topic/.DS_Store":                []byte("not a block"),
		"data/topic/backup.tar":               []byte("not a block"),
		"data/topic/123.log":                  []byte("not a block"),
		"data/notes.txt":                      []byte("not a topic"),
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
	afs := newTestAfs(t)
	// a regular file where a topic directory should be cannot be loaded as a topic
	if err := afs.WriteFile("data/notes.txt", []byte("not a topic"), 0600); err != nil {
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
	afs := newTestAfs(t)
	m, err := NewLogTopicsManager(LogTopicManagerParams{
		Store:            aferostore.New(afs, "data"),
		TTL:              time.Minute,
		CheckForNewEvery: time.Minute,
		MaxBlockSize:     1 << 20,
		MaxFrameEntries:  7,
		MaxFrameBytes:    4096,
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
	m := newTestManager(t, newTestAfs(t))

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

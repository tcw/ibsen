package manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/blockstore/aferostore"
	"github.com/tcw/ibsen/access/common"
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

func newTestManagerWithStore(t *testing.T, store common.BlockStore) *LogTopicsManager {
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

func writeTopic(t *testing.T, m *LogTopicsManager, topic string, from, count int) {
	t.Helper()
	entries := make([][]byte, count)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf("%s-%d", topic, from+i))
	}
	if err := m.Write(common.TopicName(topic), &entries); err != nil {
		t.Fatal(err)
	}
}

func readTopic(m *LogTopicsManager, topic string) ([]common.LogEntry, error) {
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	var got []common.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got = append(got, *batch...)
			wg.Done()
		}
		close(done)
	}()
	err := m.Read(ReadParams{TopicName: common.TopicName(topic), LogChan: logChan, Wg: &wg, BatchSize: 100})
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

func TestManager_loadsTopicWithStrayFiles(t *testing.T) {
	afs := newTestAfs(t)
	// a topic as a previous run left it, written directly so no background indexing is still
	// running when the manager loads it: one log block plus files that are not blocks
	var block []byte
	for i := 0; i < 30; i++ {
		block = append(block, common.CreateByteEntry([]byte(fmt.Sprintf("topic-%d", i)), common.Offset(i))...)
	}
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

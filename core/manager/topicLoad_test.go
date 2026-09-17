package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/aferostore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// gatedLoadStore counts how often a topic's log blocks are listed, which happens once per
// topic load. The first listing waits until a second one arrives or a timeout passes, so
// two concurrent loads would both be in progress at the same time.
type gatedLoadStore struct {
	driven.BlockStore
	topic    domain.TopicName
	lists    atomic.Int32
	second   chan struct{}
	gateOnce sync.Once
}

func (g *gatedLoadStore) List(topic domain.TopicName, kind driven.BlockKind) ([]driven.Block, error) {
	if topic == g.topic && kind == driven.Log {
		switch g.lists.Add(1) {
		case 1:
			select {
			case <-g.second:
			case <-time.After(200 * time.Millisecond):
			}
		case 2:
			g.gateOnce.Do(func() { close(g.second) })
		}
	}
	return g.BlockStore.List(topic, kind)
}

func TestManager_concurrentFirstRequestsLoadTopicOnce(t *testing.T) {
	afs := newTestAfs(t)
	store := &gatedLoadStore{BlockStore: aferostore.New(afs, "data"), topic: "topic", second: make(chan struct{})}
	var block []byte
	for i := 0; i < 30; i++ {
		block = append(block, domain.CreateByteEntry([]byte(fmt.Sprintf("topic-%d", i)), domain.Offset(i))...)
	}
	if _, err := store.Append(driven.LogRef("topic", 0), block); err != nil {
		t.Fatal(err)
	}
	m := newTestManagerWithStore(t, store)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(from int) {
			defer wg.Done()
			entries := [][]byte{[]byte(fmt.Sprintf("topic-%d", from))}
			errs <- m.Write("topic", &entries)
		}(30 + i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if lists := store.lists.Load(); lists != 1 {
		t.Fatalf("topic was loaded %d times, want 1", lists)
	}
	got, err := readTopic(m, "topic")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 32 {
		t.Fatalf("read %d entries, want 32", len(got))
	}
}

func TestManager_waitersOfFailedLoadGetErrorAndLaterRequestsRetry(t *testing.T) {
	afs := newTestAfs(t)
	// a regular file where the topic directory should be cannot be loaded as a topic
	if err := afs.WriteFile("data/topic", []byte("not a topic"), 0600); err != nil {
		t.Fatal(err)
	}
	m := newTestManager(t, afs)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entries := [][]byte{[]byte("x")}
			if err := m.Write("topic", &entries); err == nil {
				t.Error("write to a topic that cannot be loaded succeeded")
			}
		}()
	}
	wg.Wait()

	if err := afs.Remove("data/topic"); err != nil {
		t.Fatal(err)
	}
	writeTopic(t, m, "topic", 0, 3)
	if got, err := readTopic(m, "topic"); err != nil || len(got) != 3 {
		t.Fatalf("read %d entries with err=%v, want 3", len(got), err)
	}
}

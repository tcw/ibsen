package manager

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
)

// loadGateFs counts how often a topic directory is opened, which happens once per topic
// load. The first open waits until a second one arrives or a timeout passes, so two
// concurrent loads are both in progress at the same time.
type loadGateFs struct {
	afero.Fs
	dir      string
	opens    atomic.Int32
	second   chan struct{}
	gateOnce sync.Once
}

func (f *loadGateFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if name == f.dir {
		switch f.opens.Add(1) {
		case 1:
			select {
			case <-f.second:
			case <-time.After(200 * time.Millisecond):
			}
		case 2:
			f.gateOnce.Do(func() { close(f.second) })
		}
	}
	return f.Fs.OpenFile(name, flag, perm)
}

func TestManager_concurrentFirstRequestsLoadTopicOnce(t *testing.T) {
	fs := &loadGateFs{Fs: afero.NewMemMapFs(), dir: "data/topic", second: make(chan struct{})}
	afs := &afero.Afero{Fs: fs}
	var block []byte
	for i := 0; i < 30; i++ {
		block = append(block, common.CreateByteEntry([]byte(fmt.Sprintf("topic-%d", i)), common.Offset(i))...)
	}
	if err := afs.MkdirAll("data/topic", 0744); err != nil {
		t.Fatal(err)
	}
	if err := afs.WriteFile("data/topic/00000000000000000000.log", block, 0600); err != nil {
		t.Fatal(err)
	}
	m := newTestManager(t, afs)

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
	if opens := fs.opens.Load(); opens != 1 {
		t.Fatalf("topic was loaded %d times, want 1", opens)
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

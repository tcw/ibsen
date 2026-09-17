package manager

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
)

// gatedWriteFs holds every write to a file with the given suffix until release is closed,
// and reports the first held write on entered.
type gatedWriteFs struct {
	afero.Fs
	suffix  string
	entered chan string
	release chan struct{}
}

func newGatedWriteAfs(t *testing.T, suffix string) (*afero.Afero, *gatedWriteFs) {
	t.Helper()
	fs := &gatedWriteFs{Fs: afero.NewMemMapFs(), suffix: suffix, entered: make(chan string, 1), release: make(chan struct{})}
	afs := &afero.Afero{Fs: fs}
	if err := afs.MkdirAll("data", 0744); err != nil {
		t.Fatal(err)
	}
	return afs, fs
}

func (f *gatedWriteFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil || !strings.HasSuffix(name, f.suffix) {
		return file, err
	}
	return &gatedWriteFile{File: file, fs: f}, nil
}

type gatedWriteFile struct {
	afero.File
	fs *gatedWriteFs
}

func (g *gatedWriteFile) Write(p []byte) (int, error) {
	select {
	case g.fs.entered <- g.Name():
	default:
	}
	<-g.fs.release
	return g.File.Write(p)
}

func closeAsync(m *LogTopicsManager) <-chan struct{} {
	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	return closed
}

func assertStillWaiting(t *testing.T, closed <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-closed:
		t.Fatalf("Close returned while %s", what)
	case <-time.After(100 * time.Millisecond):
	}
}

func waitForClose(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

func TestManager_closeWaitsForInFlightWrite(t *testing.T) {
	afs, fs := newGatedWriteAfs(t, ".log")
	m := newTestManager(t, afs)
	written := make(chan error, 1)
	go func() {
		entries := make([][]byte, 30)
		for i := range entries {
			entries[i] = []byte(fmt.Sprintf("topic-%d", i))
		}
		written <- m.Write("topic", &entries)
	}()
	<-fs.entered

	closed := closeAsync(m)
	assertStillWaiting(t, closed, "a write was in flight")
	close(fs.release)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	waitForClose(t, closed)

	entries := [][]byte{[]byte("late")}
	if err := m.Write("topic", &entries); !errors.Is(err, ErrClosed) {
		t.Errorf("write after Close: err=%v, want ErrClosed", err)
	}
	if _, err := readTopic(m, "unloaded"); !errors.Is(err, ErrClosed) {
		t.Errorf("read of an unloaded topic after Close: err=%v, want ErrClosed", err)
	}
	if got, err := readTopic(m, "topic"); err != nil || len(got) != 30 {
		t.Errorf("read of a loaded topic after Close: %d entries with err=%v, want 30", len(got), err)
	}
}

func TestManager_closeWaitsForBackgroundIndexing(t *testing.T) {
	afs, fs := newGatedWriteAfs(t, ".idx")
	m := newTestManager(t, afs)
	writeTopic(t, m, "topic", 0, 30)
	<-fs.entered

	closed := closeAsync(m)
	assertStillWaiting(t, closed, "background indexing was running")
	close(fs.release)
	waitForClose(t, closed)

	// entries 0, 10 and 20 are indexed, as checksummed (offset, byteOffset) pairs
	idx, err := afs.ReadFile("data/topic/00000000000000000000.idx")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 3*index.PairSize {
		t.Fatalf("index has %d bytes after Close, want %d", len(idx), 3*index.PairSize)
	}
	var entries = [][]byte{[]byte("late")}
	if err := m.Write(domain.TopicName("topic"), &entries); !errors.Is(err, ErrClosed) {
		t.Errorf("write after Close: err=%v, want ErrClosed", err)
	}
}

func TestManager_closeTwice(t *testing.T) {
	m := newTestManager(t, newTestAfs(t))
	writeTopic(t, m, "topic", 0, 3)
	waitForClose(t, closeAsync(m))
	waitForClose(t, closeAsync(m))
}

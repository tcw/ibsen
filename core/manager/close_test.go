package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
)

// gatedWriteFs holds every write to a file with the given suffix until release is closed,
// and reports the first held write on entered.
type gatedWriteFs struct {
	filestore.FS
	suffix  string
	entered chan string
	release chan struct{}
}

func newGatedWriteFs(t *testing.T, suffix string) (string, *gatedWriteFs) {
	t.Helper()
	return t.TempDir(), &gatedWriteFs{
		FS: filestore.OS{}, suffix: suffix,
		entered: make(chan string, 1), release: make(chan struct{}),
	}
}

func (f *gatedWriteFs) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil || !strings.HasSuffix(name, f.suffix) {
		return file, err
	}
	return &gatedWriteFile{File: file, name: name, fs: f}, nil
}

type gatedWriteFile struct {
	filestore.File
	name string
	fs   *gatedWriteFs
}

func (g *gatedWriteFile) Write(p []byte) (int, error) {
	select {
	case g.fs.entered <- g.name:
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
	root, fs := newGatedWriteFs(t, ".log")
	m := newTestManagerWithStore(t, filestore.New(fs, root))
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
	root, fs := newGatedWriteFs(t, ".idx")
	m := newTestManagerWithStore(t, filestore.New(fs, root))
	writeTopic(t, m, "topic", 0, 30)
	<-fs.entered

	closed := closeAsync(m)
	assertStillWaiting(t, closed, "background indexing was running")
	close(fs.release)
	waitForClose(t, closed)

	// the thirty entries went in one write, so they are one frame, and a pair points at a
	// frame start: one checksummed (offset, byteOffset) pair is the whole index for them
	idx, err := os.ReadFile(filepath.Join(root, "topic", "00000000000000000000.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != index.PairSize {
		t.Fatalf("index has %d bytes after Close, want %d", len(idx), index.PairSize)
	}
	var entries = [][]byte{[]byte("late")}
	if err := m.Write(domain.TopicName("topic"), &entries); !errors.Is(err, ErrClosed) {
		t.Errorf("write after Close: err=%v, want ErrClosed", err)
	}
}

func TestManager_closeTwice(t *testing.T) {
	m := newTestManager(t, newTestRoot(t))
	writeTopic(t, m, "topic", 0, 3)
	waitForClose(t, closeAsync(m))
	waitForClose(t, closeAsync(m))
}

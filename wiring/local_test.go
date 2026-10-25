package wiring

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
)

func openLocal(t *testing.T, params LocalParams) *LocalLog {
	t.Helper()
	local, err := OpenLocal(params)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(local.Close)
	return local
}

func writeEntries(t *testing.T, log driver.LogManager, topic string, entries ...string) {
	t.Helper()
	batch := make([][]byte, len(entries))
	for i, entry := range entries {
		batch[i] = []byte(entry)
	}
	if err := log.Write(domain.TopicName(topic), &batch); err != nil {
		t.Fatal(err)
	}
}

func readEntries(t *testing.T, log driver.LogManager, topic string) []string {
	t.Helper()
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var entries []string
	done := make(chan error, 1)
	go func() {
		done <- log.Read(driver.ReadParams{
			TopicName: domain.TopicName(topic),
			From:      0,
			BatchSize: 100,
			LogChan:   logChan,
			Wg:        &wg,
		})
		close(logChan)
	}()
	for batch := range logChan {
		for _, entry := range *batch {
			entries = append(entries, string(entry.Entry))
		}
		wg.Done()
	}
	if err := <-done; err != nil && !errors.Is(err, domain.NoEntriesFound) {
		t.Fatal(err)
	}
	return entries
}

// TestOpenLocalKeepsWhatItWrote: the point of opening a directory without a server is that
// the bytes are still there afterwards.
func TestOpenLocalKeepsWhatItWrote(t *testing.T) {
	root := t.TempDir()
	first, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, first, "greetings", "hello", "world")
	first.Close()

	second := openLocal(t, LocalParams{RootPath: root, ReadOnly: true})
	if got := readEntries(t, second, "greetings"); strings.Join(got, ",") != "hello,world" {
		t.Errorf("read %v, want [hello world]", got)
	}
}

// TestSecondWriterIsRefused: a writable open takes the same lease the server takes, so two of
// them on one directory is the outcome the lock exists to prevent. The lease is released on
// Close, so the next one gets it.
func TestSecondWriterIsRefused(t *testing.T) {
	root := t.TempDir()
	first, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenLocal(LocalParams{RootPath: root})
	if !errors.Is(err, ErrWriteLockUnavailable) {
		if err == nil {
			second.Close()
		}
		t.Fatalf("second writable open gave %v, want ErrWriteLockUnavailable", err)
	}
	first.Close()

	third, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatalf("open after the first was closed gave %v, want the lease", err)
	}
	third.Close()
}

// TestReadOnlyOpenTakesNoLock is what makes `ibsen cat` usable at all: a reader must be able
// to look at a directory somebody else is writing, which is the normal case while a server
// is running.
func TestReadOnlyOpenTakesNoLock(t *testing.T) {
	root := t.TempDir()
	writer, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writeEntries(t, writer, "live", "one", "two")

	reader := openLocal(t, LocalParams{RootPath: root, ReadOnly: true})
	if got := readEntries(t, reader, "live"); strings.Join(got, ",") != "one,two" {
		t.Errorf("read %v while the writer held the directory, want [one two]", got)
	}
}

// fileState is what a directory holds, to the detail that matters here: which files there
// are and how big each one is.
func fileState(t *testing.T, root string) []string {
	t.Helper()
	var state []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			state = append(state, rel+"/")
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		state = append(state, fmt.Sprintf("%s:%d", rel, info.Size()))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(state)
	return state
}

// TestReadOnlyOpenWritesNothing is the claim a reader rests on. Loading a topic recovers its
// head block and could truncate a torn tail; pointed at a directory another instance owns,
// that would be somebody else's log being rewritten. The read-only store refuses every call
// that would change a file, so what is on disk before a read is what is on disk after it —
// including the lock file, which a reader never creates.
func TestReadOnlyOpenWritesNothing(t *testing.T) {
	root := t.TempDir()
	writer, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, writer, "topic", "one", "two", "three")
	writer.Close()

	before := fileState(t, root)
	reader := openLocal(t, LocalParams{RootPath: root, ReadOnly: true})
	if got := readEntries(t, reader, "topic"); len(got) != 3 {
		t.Fatalf("read %v, want three entries", got)
	}
	if after := fileState(t, root); strings.Join(after, "|") != strings.Join(before, "|") {
		t.Errorf("a read-only open changed the directory:\nbefore %v\nafter  %v", before, after)
	}
}

// TestReadOnlyOpenRefusesWrites: the store cannot change a file, and the manager turns writes
// away before they reach it. Both, because they cover different things: the manager covers
// the write, the store covers what loading and indexing would do behind it.
func TestReadOnlyOpenRefusesWrites(t *testing.T) {
	root := t.TempDir()
	writer, err := OpenLocal(LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, writer, "topic", "one")
	writer.Close()

	reader := openLocal(t, LocalParams{RootPath: root, ReadOnly: true})
	batch := [][]byte{[]byte("nope")}
	if err := reader.Write("topic", &batch); err == nil {
		t.Fatal("a read-only open accepted a write")
	}
}

// TestZstdFramesAreReadWithoutAskingForZstd: compression chooses only what is written, and
// the read registry holds every codec the binary links, so a plain reopen reads them.
func TestZstdFramesAreReadWithoutAskingForZstd(t *testing.T) {
	root := t.TempDir()
	writer, err := OpenLocal(LocalParams{RootPath: root, Compression: "zstd"})
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, writer, "compressed", "compress me please", "and me as well")
	writer.Close()

	reader := openLocal(t, LocalParams{RootPath: root, ReadOnly: true})
	if got := readEntries(t, reader, "compressed"); strings.Join(got, ",") != "compress me please,and me as well" {
		t.Errorf("read %v, want the entries zstd wrote", got)
	}
}

// TestUnknownCompressionIsRefused, and refused before the lease is taken, so a mistyped flag
// does not leave a lock file behind for the next run to wait on.
func TestUnknownCompressionIsRefused(t *testing.T) {
	root := t.TempDir()
	local, err := OpenLocal(LocalParams{RootPath: root, Compression: "snappy"})
	if !errors.Is(err, ErrUnknownCompression) {
		if err == nil {
			local.Close()
		}
		t.Fatalf("open gave %v, want ErrUnknownCompression", err)
	}
	if _, err := os.Stat(filepath.Join(root, writeLockFileName)); !os.IsNotExist(err) {
		t.Errorf("a refused open left a lock file behind")
	}
}

func TestMissingRootIsRefused(t *testing.T) {
	local, err := OpenLocal(LocalParams{RootPath: filepath.Join(t.TempDir(), "nope")})
	if err == nil {
		local.Close()
		t.Fatal("opening a directory that does not exist was accepted")
	}
}

// TestInjectedLockIsUsed pins the injection point: a program embedding the log brings its own
// coordination, and a refusal from it is the same refusal the file lease gives.
func TestInjectedLockIsUsed(t *testing.T) {
	root := t.TempDir()
	refusing := &countingLock{acquire: false}
	local, err := OpenLocal(LocalParams{RootPath: root, Lock: refusing})
	if !errors.Is(err, ErrWriteLockUnavailable) {
		if err == nil {
			local.Close()
		}
		t.Fatalf("open gave %v, want ErrWriteLockUnavailable", err)
	}

	granting := &countingLock{acquire: true}
	opened, err := OpenLocal(LocalParams{RootPath: root, Lock: granting})
	if err != nil {
		t.Fatal(err)
	}
	opened.Close()
	if granting.acquired != 1 || granting.released != 1 {
		t.Errorf("injected lock was acquired %d and released %d times, want 1 and 1",
			granting.acquired, granting.released)
	}
	if _, err := os.Stat(filepath.Join(root, writeLockFileName)); !os.IsNotExist(err) {
		t.Errorf("an injected lock was used and a file lease was taken as well")
	}
}

type countingLock struct {
	acquire  bool
	acquired int
	released int
}

func (c *countingLock) AcquireLock() bool {
	c.acquired++
	return c.acquire
}

func (c *countingLock) ReleaseLock() bool {
	c.released++
	return true
}

// TestLocalLogSatisfiesTheDrivingPort: a driving adapter cannot tell a directory opened like
// this apart from the log a server holds, which is what lets stdio drive either.
func TestLocalLogSatisfiesTheDrivingPort(t *testing.T) {
	var _ driver.LogManager = (*LocalLog)(nil)
	root := t.TempDir()
	local := openLocal(t, LocalParams{RootPath: root})
	writeEntries(t, local, "topic", "entry")
	var names bytes.Buffer
	for _, name := range local.List() {
		names.WriteString(string(name))
	}
	if names.String() != "topic" {
		t.Errorf("List gave %q, want topic", names.String())
	}
}

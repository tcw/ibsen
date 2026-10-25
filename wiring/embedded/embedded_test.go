package embedded

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
)

func openTestLog(t *testing.T, params Params) *Log {
	t.Helper()
	if params.Store == nil {
		params.Store = memstore.New()
	}
	log, err := Open(params)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(log.Close)
	return log
}

func write(t *testing.T, log *Log, topic string, from, count int) {
	t.Helper()
	batch := make([][]byte, count)
	for i := range batch {
		batch[i] = []byte(fmt.Sprintf("%s-%d", topic, from+i))
	}
	if err := log.Write(domain.TopicName(topic), &batch); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, log driver.LogManager, topic string, from domain.Offset) []domain.LogEntry {
	t.Helper()
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
	err := log.Read(driver.ReadParams{
		TopicName: domain.TopicName(topic), LogChan: logChan, Wg: &wg, From: from, BatchSize: 10,
	})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		t.Fatalf("read %s from %d: %v", topic, from, err)
	}
	return got
}

// Where the bytes go is the one decision this package will not make for a program, since
// guessing would mean choosing a dependency on its behalf.
func TestOpenRequiresAStore(t *testing.T) {
	log, err := Open(Params{})

	if !errors.Is(err, ErrNoStore) {
		t.Fatalf("got %v, want ErrNoStore", err)
	}
	if log != nil {
		t.Error("a log was returned alongside the error")
	}
}

// The whole of it: write, list, read back, with nothing wired but a store.
func TestAnEmbeddedLogWritesAndReads(t *testing.T) {
	log := openTestLog(t, Params{})

	write(t, log, "events", 0, 5)
	write(t, log, "audit", 0, 2)

	topics := log.List()
	if len(topics) != 2 {
		t.Fatalf("List gave %v, want two topics", topics)
	}
	got := readAll(t, log, "events", 0)
	if len(got) != 5 {
		t.Fatalf("read %d entries, want 5", len(got))
	}
	for i, entry := range got {
		if entry.Offset != uint64(i) || string(entry.Entry) != fmt.Sprintf("events-%d", i) {
			t.Errorf("entry %d came back as offset %d, %q", i, entry.Offset, entry.Entry)
		}
	}
	if from3 := readAll(t, log, "events", 3); len(from3) != 2 {
		t.Errorf("read from offset 3 gave %d entries, want 2", len(from3))
	}
}

// An embedded program drives the same port a gRPC server does, so code written against one
// works against the other without knowing which it has.
func TestAnEmbeddedLogIsADriverLogManager(t *testing.T) {
	var port driver.LogManager = openTestLog(t, Params{})

	entries := [][]byte{[]byte("through the port")}
	if err := port.Write("events", &entries); err != nil {
		t.Fatal(err)
	}

	if got := readAll(t, port, "events", 0); len(got) != 1 {
		t.Fatalf("read %d entries through the port, want 1", len(got))
	}
}

// A server defaults to a gigabyte a block; the sort of device this package exists for does
// not have one.
func TestZeroMaxBlockSizeMeansTheEmbeddedDefault(t *testing.T) {
	log := openTestLog(t, Params{})

	if got := log.manager.Params.MaxBlockSize; got != DefaultMaxBlockSize {
		t.Errorf("block size is %d, want the embedded default %d", got, DefaultMaxBlockSize)
	}
}

// Everything else is passed through to the core untouched, so an embedded build can reach
// every knob a server can.
func TestParamsReachTheCore(t *testing.T) {
	store := memstore.New()
	log := openTestLog(t, Params{
		Store:           store,
		ReadOnly:        true,
		MaxBlockSize:    4096,
		IndexSparsity:   3,
		MaxFrameEntries: 7,
		MaxFrameBytes:   512,
		FlushEntries:    9,
		Codec:           driven.NoCodec{},
	})

	got := log.manager.Params
	want := manager.LogTopicManagerParams{
		ReadOnly: true, Store: store, MaxBlockSize: 4096, IndexSparsity: 3,
		MaxFrameEntries: 7, MaxFrameBytes: 512, FlushEntries: 9, Codec: driven.NoCodec{},
	}
	if got.ReadOnly != want.ReadOnly || got.MaxBlockSize != want.MaxBlockSize ||
		got.IndexSparsity != want.IndexSparsity || got.MaxFrameEntries != want.MaxFrameEntries ||
		got.MaxFrameBytes != want.MaxFrameBytes || got.FlushEntries != want.FlushEntries ||
		got.Codec != want.Codec {
		t.Errorf("the core got %+v, want the params Open was given", got)
	}
}

// Close stops the indexer and refuses further writes, so a program that embeds the log can
// shut it down and know nothing is still touching storage.
func TestCloseRefusesFurtherWrites(t *testing.T) {
	log, err := Open(Params{Store: memstore.New()})
	if err != nil {
		t.Fatal(err)
	}
	write(t, log, "events", 0, 3)

	log.Close()

	entries := [][]byte{[]byte("after close")}
	if err := log.Write("events", &entries); !errors.Is(err, manager.ErrClosed) {
		t.Errorf("write after Close gave %v, want ErrClosed", err)
	}
	// a loaded topic stays readable, which is what lets a program drain before it exits
	if got := readAll(t, log, "events", 0); len(got) != 3 {
		t.Errorf("read %d entries after Close, want the 3 written", len(got))
	}
}

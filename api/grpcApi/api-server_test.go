package grpcApi

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/blockstore/aferostore"
	"github.com/tcw/ibsen/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeReadStream is an Ibsen_ReadServer whose Send is supplied by the test.
type fakeReadStream struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*OutputEntries) error
}

func (f *fakeReadStream) Context() context.Context {
	return f.ctx
}

func (f *fakeReadStream) Send(out *OutputEntries) error {
	return f.send(out)
}

const testTopic = "topic"

func testPayload(offset int) string {
	return fmt.Sprintf("entry-%d", offset)
}

func newTestServer(t *testing.T, entries int) server {
	t.Helper()
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("data", 0744); err != nil {
		t.Fatal(err)
	}
	logManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store:        aferostore.New(afs, "data"),
		MaxBlockSize: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := server{manager: &logManager, TTL: time.Minute, CheckForNewEvery: time.Millisecond}
	writeTestEntries(t, s, 0, entries)
	return s
}

func writeTestEntries(t *testing.T, s server, from, count int) {
	t.Helper()
	entries := make([][]byte, count)
	for i := range entries {
		entries[i] = []byte(testPayload(from + i))
	}
	if err := s.manager.Write(testTopic, &entries); err != nil {
		t.Fatal(err)
	}
}

func startRead(s server, params *ReadParams, stream Ibsen_ReadServer) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- s.Read(params, stream)
	}()
	return done
}

func waitForRead(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not return")
		return nil
	}
}

// collector records the entries a fake stream receives.
type collector struct {
	mu      sync.Mutex
	entries []*Entry
}

func (c *collector) send(out *OutputEntries) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, out.Entries...)
	return nil
}

func (c *collector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *collector) checkContiguous(from, to int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != to-from {
		return fmt.Errorf("received %d entries, want %d", len(c.entries), to-from)
	}
	for i, entry := range c.entries {
		if entry.Offset != uint64(from+i) || string(entry.Content) != testPayload(from+i) {
			return fmt.Errorf("entry %d is offset %d %q, want offset %d", i, entry.Offset, entry.Content, from+i)
		}
	}
	return nil
}

// stableGoroutineCount waits for background goroutines, such as async indexing, to settle.
func stableGoroutineCount() int {
	last := runtime.NumGoroutine()
	for i := 0; i < 40; i++ {
		time.Sleep(50 * time.Millisecond)
		current := runtime.NumGoroutine()
		if current == last {
			return current
		}
		last = current
	}
	return last
}

func TestRead_streamsFromOffset(t *testing.T) {
	s := newTestServer(t, 500)
	for _, from := range []int{0, 1, 137, 499} {
		t.Run(fmt.Sprintf("from=%d", from), func(t *testing.T) {
			c := &collector{}
			stream := &fakeReadStream{ctx: context.Background(), send: c.send}
			params := &ReadParams{Topic: testTopic, Offset: uint64(from), BatchSize: 7, StopOnCompletion: true}
			if err := waitForRead(t, startRead(s, params, stream)); err != nil {
				t.Fatal(err)
			}
			if err := c.checkContiguous(from, 500); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRead_returnsWhenSendFails(t *testing.T) {
	for _, batchSize := range []uint32{7, 1000} {
		t.Run(fmt.Sprintf("batchSize=%d", batchSize), func(t *testing.T) {
			s := newTestServer(t, 500)
			sendErr := errors.New("connection reset")
			stream := &fakeReadStream{ctx: context.Background(), send: func(*OutputEntries) error {
				return sendErr
			}}
			params := &ReadParams{Topic: testTopic, BatchSize: batchSize}
			if err := waitForRead(t, startRead(s, params, stream)); !errors.Is(err, sendErr) {
				t.Fatalf("err=%v, want the send error", err)
			}
		})
	}
}

func TestRead_returnsWhenClientGoesAway(t *testing.T) {
	s := newTestServer(t, 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &fakeReadStream{ctx: ctx, send: func(*OutputEntries) error {
		cancel()
		return nil
	}}
	params := &ReadParams{Topic: testTopic, BatchSize: 7}
	if err := waitForRead(t, startRead(s, params, stream)); err == nil {
		t.Fatal("Read returned nil after the client went away")
	}
}

func TestRead_returnsWhenClientGoesAwayWhileTailing(t *testing.T) {
	s := newTestServer(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &collector{}
	stream := &fakeReadStream{ctx: ctx, send: c.send}
	done := startRead(s, &ReadParams{Topic: testTopic, BatchSize: 7}, stream)
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := waitForRead(t, done); err == nil {
		t.Fatal("Read returned nil after the client went away")
	}
}

func TestRead_tailingDoesNotLeakGoroutines(t *testing.T) {
	s := newTestServer(t, 100)
	s.TTL = 300 * time.Millisecond
	before := stableGoroutineCount()
	c := &collector{}
	stream := &fakeReadStream{ctx: context.Background(), send: c.send}
	// polls for new entries every millisecond until the TTL expires
	if err := waitForRead(t, startRead(s, &ReadParams{Topic: testTopic, BatchSize: 7}, stream)); err != nil {
		t.Fatal(err)
	}
	if err := c.checkContiguous(0, 100); err != nil {
		t.Fatal(err)
	}
	if after := stableGoroutineCount(); after > before+2 {
		t.Fatalf("%d goroutines before read, %d after", before, after)
	}
}

func TestRead_tailingReceivesNewEntries(t *testing.T) {
	s := newTestServer(t, 50)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &collector{}
	stream := &fakeReadStream{ctx: ctx, send: c.send}
	done := startRead(s, &ReadParams{Topic: testTopic, BatchSize: 7}, stream)

	waitUntil := func(n int) {
		deadline := time.Now().Add(5 * time.Second)
		for c.len() < n {
			if time.Now().After(deadline) {
				t.Fatalf("received %d entries, want %d", c.len(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitUntil(50)
	writeTestEntries(t, s, 50, 30)
	waitUntil(80)
	cancel()
	waitForRead(t, done)
	if err := c.checkContiguous(0, 80); err != nil {
		t.Fatal(err)
	}
}

func TestWriteAndRead_rejectInvalidTopicName(t *testing.T) {
	s := newTestServer(t, 1)
	_, err := s.Write(context.Background(), &InputEntries{Topic: "../escaped", Entries: [][]byte{[]byte("x")}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("write err=%v, want InvalidArgument", err)
	}
	stream := &fakeReadStream{ctx: context.Background(), send: func(*OutputEntries) error { return nil }}
	err = waitForRead(t, startRead(s, &ReadParams{Topic: "../escaped", BatchSize: 10}, stream))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("read err=%v, want InvalidArgument", err)
	}
}

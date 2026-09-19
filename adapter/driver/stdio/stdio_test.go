// The tests are an external package because this one drives the log and a test has to wire
// something for it to drive: a test is its own composition root, which is the exemption
// scripts/check-architecture.sh makes for exactly this.
package stdio_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/wiring/embedded"
)

func openLog(t *testing.T) *embedded.Log {
	t.Helper()
	log, err := embedded.Open(embedded.Params{Store: memstore.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(log.Close)
	return log
}

// appendString puts a stream into a topic and returns how many entries it made.
func appendString(t *testing.T, log driver.LogManager, topic, stream string, params stdio.AppendParams) uint64 {
	t.Helper()
	written, err := stdio.Append(log, domain.TopicName(topic), strings.NewReader(stream), params)
	if err != nil {
		t.Fatal(err)
	}
	return written
}

func catString(t *testing.T, log driver.LogManager, topic string, params stdio.CatParams) string {
	t.Helper()
	var out bytes.Buffer
	if _, err := stdio.Cat(log, domain.TopicName(topic), &out, params); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// TestStreamRoundTripsThroughTheLog is the whole point of the adapter: what goes in comes out.
func TestStreamRoundTripsThroughTheLog(t *testing.T) {
	log := openLog(t)
	stream := "hello\nworld\nthird entry with spaces\n"
	if written := appendString(t, log, "greetings", stream, stdio.AppendParams{}); written != 3 {
		t.Fatalf("wrote %d entries, want 3", written)
	}
	if out := catString(t, log, "greetings", stdio.CatParams{}); out != stream {
		t.Errorf("cat gave %q, want %q", out, stream)
	}
}

// TestEmptyLineIsAnEmptyEntry pins the one place this adapter disagrees with the gRPC client,
// which skips blank lines. A stream that comes back with fewer entries than went in is not a
// round trip, and an empty entry is something the log can hold.
func TestEmptyLineIsAnEmptyEntry(t *testing.T) {
	log := openLog(t)
	if written := appendString(t, log, "blanks", "a\n\nb\n", stdio.AppendParams{}); written != 3 {
		t.Fatalf("wrote %d entries, want 3", written)
	}
	out := catString(t, log, "blanks", stdio.CatParams{Offsets: true})
	want := "0\ta\n1\t\n2\tb\n"
	if out != want {
		t.Errorf("cat gave %q, want %q", out, want)
	}
}

// TestLengthFramingCarriesWhatLinesCannot is why the second framing exists.
func TestLengthFramingCarriesWhatLinesCannot(t *testing.T) {
	log := openLog(t)
	entries := [][]byte{[]byte("holds\na newline"), {}, {0x00, 0xff, 0x0a, 0x7f}}
	var stream bytes.Buffer
	for _, entry := range entries {
		var header [8]byte
		binary.LittleEndian.PutUint64(header[:], uint64(len(entry)))
		stream.Write(header[:])
		stream.Write(entry)
	}
	written, err := stdio.Append(log, "binary", bytes.NewReader(stream.Bytes()),
		stdio.AppendParams{Framing: stdio.Length})
	if err != nil {
		t.Fatal(err)
	}
	if written != uint64(len(entries)) {
		t.Fatalf("wrote %d entries, want %d", written, len(entries))
	}
	var out bytes.Buffer
	if _, err := stdio.Cat(log, "binary", &out, stdio.CatParams{Framing: stdio.Length}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), stream.Bytes()) {
		t.Errorf("cat gave %q, want %q", out.Bytes(), stream.Bytes())
	}
}

// TestTruncatedLengthFrameIsReported: a stream cut inside an entry is a cut stream, not the
// end of one. The entries before the cut are written, because they were whole, and that does
// not depend on the batch size: a batch boundary is a tuning choice, not a statement about
// the stream.
func TestTruncatedLengthFrameIsReported(t *testing.T) {
	var stream bytes.Buffer
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], 2)
	stream.Write(header[:])
	stream.WriteString("ok")
	binary.LittleEndian.PutUint64(header[:], 10)
	stream.Write(header[:])
	stream.WriteString("cut")

	for _, batch := range []int{1, 1000} {
		t.Run(fmt.Sprintf("batch-%d", batch), func(t *testing.T) {
			log := openLog(t)
			written, err := stdio.Append(log, "cut", bytes.NewReader(stream.Bytes()),
				stdio.AppendParams{Framing: stdio.Length, BatchSize: batch})
			if !errors.Is(err, stdio.ErrTruncatedEntry) {
				t.Fatalf("append gave %v, want ErrTruncatedEntry", err)
			}
			if written != 1 {
				t.Fatalf("wrote %d entries before the cut, want 1", written)
			}
			if out := catString(t, log, "cut", stdio.CatParams{}); out != "ok\n" {
				t.Errorf("cat gave %q, want the whole entry that arrived before the cut", out)
			}
		})
	}
}

// TestOffsetsNeedLines: length framing carries entries and nothing else, so offsets have
// nowhere to go and asking for them is a mistake rather than a thing quietly ignored.
func TestOffsetsNeedLines(t *testing.T) {
	log := openLog(t)
	appendString(t, log, "topic", "a\n", stdio.AppendParams{})
	_, err := stdio.Cat(log, "topic", &bytes.Buffer{}, stdio.CatParams{Framing: stdio.Length, Offsets: true})
	if !errors.Is(err, stdio.ErrOffsetsNeedLines) {
		t.Fatalf("cat gave %v, want ErrOffsetsNeedLines", err)
	}
}

func TestCatStartsAtTheOffsetAsked(t *testing.T) {
	log := openLog(t)
	appendString(t, log, "numbers", "0\n1\n2\n3\n4\n", stdio.AppendParams{})
	if out := catString(t, log, "numbers", stdio.CatParams{From: 3}); out != "3\n4\n" {
		t.Errorf("cat from 3 gave %q, want %q", out, "3\n4\n")
	}
}

// TestBatchSizeDoesNotChangeTheStream: batching is about how many entries share a Write, and
// nothing else. The boundaries are the interesting sizes: one below, exactly, one above.
func TestBatchSizeDoesNotChangeTheStream(t *testing.T) {
	var stream strings.Builder
	for i := 0; i < 7; i++ {
		fmt.Fprintf(&stream, "entry-%d\n", i)
	}
	for _, batch := range []int{1, 3, 7, 8, 1000} {
		t.Run(fmt.Sprintf("batch-%d", batch), func(t *testing.T) {
			log := openLog(t)
			written := appendString(t, log, "topic", stream.String(), stdio.AppendParams{BatchSize: batch})
			if written != 7 {
				t.Fatalf("wrote %d entries, want 7", written)
			}
			if out := catString(t, log, "topic", stdio.CatParams{}); out != stream.String() {
				t.Errorf("cat gave %q, want %q", out, stream.String())
			}
		})
	}
}

// countingLog counts the writes a stream is turned into. It is the cost side of the batch
// size, which the test below states: the stream that comes out is the same whatever the
// batching, but the number of appends it took is not.
type countingLog struct {
	driver.LogManager
	writes int
}

func (c *countingLog) Write(topic domain.TopicName, entries domain.EntriesPtr) error {
	c.writes++
	return c.LogManager.Write(topic, entries)
}

// TestBatchSizeIsHowManyEntriesShareAWrite pins what the batch size buys. One Write is one
// append and, on a store that syncs, one flush, so this is the number of fsyncs a stream
// costs: appending a million lines one thousand at a time is a thousand of them. Nothing
// about durability changes with it — every Write that returned is on durable media — which
// is why the size is the throughput dial for a stream and the flush policy is not: a stream
// is one writer, and there is never anybody else's batch for its entries to join.
func TestBatchSizeIsHowManyEntriesShareAWrite(t *testing.T) {
	var stream strings.Builder
	for i := 0; i < 7; i++ {
		fmt.Fprintf(&stream, "entry-%d\n", i)
	}
	for _, test := range []struct{ batch, writes int }{{1, 7}, {3, 3}, {7, 1}, {8, 1}, {1000, 1}} {
		t.Run(fmt.Sprintf("batch-%d", test.batch), func(t *testing.T) {
			log := &countingLog{LogManager: openLog(t)}
			appendString(t, log, "topic", stream.String(), stdio.AppendParams{BatchSize: test.batch})
			if log.writes != test.writes {
				t.Fatalf("7 entries in batches of %d took %d writes, want %d",
					test.batch, log.writes, test.writes)
			}
		})
	}
}

// TestBatchIsBoundedByBytesAsWellAsEntries: the batch size counts entries, and entries have
// no size, so a stream of large ones would otherwise hold the batch size times the largest
// of them before writing any of it. The byte bound is what makes a larger default entry
// count safe to ship.
func TestBatchIsBoundedByBytesAsWellAsEntries(t *testing.T) {
	const entrySize = 1 << 20
	const entries = 20
	var stream strings.Builder
	stream.Grow(entries * (entrySize + 1))
	for i := 0; i < entries; i++ {
		stream.WriteString(strings.Repeat("a", entrySize))
		stream.WriteByte('\n')
	}
	log := &countingLog{LogManager: openLog(t)}

	written := appendString(t, log, "big", stream.String(), stdio.AppendParams{BatchSize: entries})

	if written != entries {
		t.Fatalf("wrote %d entries, want %d", written, entries)
	}
	// 20 MiB of entries against a 16 MiB bound is two writes, however many entries were asked for
	if log.writes != 2 {
		t.Fatalf("%d MiB of entries took %d writes, want 2 against the %d MiB bound",
			entries, log.writes, stdio.MaxBatchBytes>>20)
	}
}

// One entry larger than the byte bound is still written, in a batch of its own: a bound on
// how much is held is not a bound on what may be appended. It takes length framing to get
// there, since a line is bounded at MaxLineSize well below it.
func TestAnEntryLargerThanTheByteBoundIsStillWritten(t *testing.T) {
	entries := [][]byte{
		bytes.Repeat([]byte("b"), stdio.MaxBatchBytes+1024),
		[]byte("small"),
	}
	var stream bytes.Buffer
	for _, entry := range entries {
		var header [8]byte
		binary.LittleEndian.PutUint64(header[:], uint64(len(entry)))
		stream.Write(header[:])
		stream.Write(entry)
	}
	log := &countingLog{LogManager: openLog(t)}

	written, err := stdio.Append(log, "big", bytes.NewReader(stream.Bytes()),
		stdio.AppendParams{Framing: stdio.Length, BatchSize: 1000})
	if err != nil {
		t.Fatal(err)
	}

	if written != 2 {
		t.Fatalf("wrote %d entries, want 2", written)
	}
	if log.writes != 2 {
		t.Fatalf("took %d writes, want the large entry to have had one of its own", log.writes)
	}
	var out bytes.Buffer
	if _, err := stdio.Cat(log, "big", &out, stdio.CatParams{Framing: stdio.Length}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), stream.Bytes()) {
		t.Fatalf("the stream came back %d bytes long, want %d", out.Len(), stream.Len())
	}
}

// failingWriter fails every write after the first, which is what a closed pipe looks like
// from inside the process.
type failingWriter struct {
	mu     sync.Mutex
	writes int
	err    error
}

func (f *failingWriter) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.writes > 1 {
		return 0, f.err
	}
	return len(p), nil
}

// TestFailedWriteCancelsTheRead is the `ibsen cat topic | head -5` case. The read must be
// cancelled and its batches drained, not left blocking on a consumer that has gone: that is
// the bug the gRPC handler had, and the reason the read takes a Cancel channel at all.
func TestFailedWriteCancelsTheRead(t *testing.T) {
	log := openLog(t)
	var stream strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&stream, "entry-%d\n", i)
	}
	appendString(t, log, "big", stream.String(), stdio.AppendParams{})

	broken := &failingWriter{err: errors.New("broken pipe")}
	done := make(chan error, 1)
	go func() {
		_, err := stdio.Cat(log, "big", broken, stdio.CatParams{BatchSize: 10, Follow: true})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, broken.err) {
			t.Fatalf("cat gave %v, want the write error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cat did not return after the stream failed")
	}
}

// TestFollowPicksUpEntriesAndStopsOnCancel: a follow returns only when it is told to, and
// sees what is written while it waits.
func TestFollowPicksUpEntriesAndStopsOnCancel(t *testing.T) {
	log := openLog(t)
	appendString(t, log, "tail", "first\n", stdio.AppendParams{})

	cancel := make(chan struct{})
	out := &syncBuffer{}
	done := make(chan error, 1)
	go func() {
		_, err := stdio.Cat(log, "tail", out, stdio.CatParams{
			Follow:       true,
			PollInterval: time.Millisecond,
			Cancel:       cancel,
		})
		done <- err
	}()

	waitFor(t, func() bool { return out.String() == "first\n" }, "the entry written before the follow")
	appendString(t, log, "tail", "second\n", stdio.AppendParams{})
	waitFor(t, func() bool { return out.String() == "first\nsecond\n" }, "the entry written during the follow")

	close(cancel)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cat gave %v, want nil after a cancel", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cat did not return after the cancel")
	}
}

// TestCatWithoutFollowStopsAtTheEndOfTheLog, including a topic with nothing in it: the end of
// a log is not a failure.
func TestCatWithoutFollowStopsAtTheEndOfTheLog(t *testing.T) {
	log := openLog(t)
	empty := make([][]byte, 0)
	if err := log.Write("empty", &empty); err != nil {
		t.Fatal(err)
	}
	if out := catString(t, log, "empty", stdio.CatParams{}); out != "" {
		t.Errorf("cat of an empty topic gave %q, want nothing", out)
	}
}

func TestListWritesOneTopicPerLine(t *testing.T) {
	log := openLog(t)
	appendString(t, log, "alpha", "a\n", stdio.AppendParams{})
	appendString(t, log, "beta", "b\n", stdio.AppendParams{})
	var out bytes.Buffer
	if err := stdio.List(log, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(out.String())
	if len(lines) != 2 {
		t.Fatalf("list gave %q, want two topics", out.String())
	}
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(out.String(), want+"\n") {
			t.Errorf("list %q is missing %s on a line of its own", out.String(), want)
		}
	}
}

// TestUnknownFramingIsRefused: a framing nobody implements must not quietly become the
// default one.
func TestUnknownFramingIsRefused(t *testing.T) {
	log := openLog(t)
	if _, err := stdio.Append(log, "topic", strings.NewReader("a\n"),
		stdio.AppendParams{Framing: stdio.Framing(42)}); err == nil {
		t.Fatal("append with an unknown framing was accepted")
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func waitFor(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

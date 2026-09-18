package zstd

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/topic"
)

// These drive a real Topic through the port, since round-tripping a byte slice says nothing
// about whether a log written with this codec can be read back, reloaded and recovered.

func payload(offset int) []byte {
	return []byte(fmt.Sprintf(`{"event":"order-placed","offset":%d,"amount":99,"currency":"NOK"}`, offset))
}

func writeTo(t *testing.T, tp *topic.Topic, from, count int) {
	t.Helper()
	batch := make([][]byte, count)
	for i := range batch {
		batch[i] = payload(from + i)
	}
	if err := tp.Write(&batch); err != nil {
		t.Fatal(err)
	}
}

func readFrom(t *testing.T, tp *topic.Topic, from domain.Offset) []domain.LogEntry {
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
	err := tp.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: 7})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		t.Fatalf("read from %d: %v", from, err)
	}
	return got
}

func blockBytes(t *testing.T, store driven.BlockStore, name domain.TopicName) int64 {
	t.Helper()
	blocks, err := store.List(name, driven.Log)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, block := range blocks {
		total = total + block.Size
	}
	return total
}

func newTopic(t *testing.T, store driven.BlockStore, name string, codec driven.Codec, codecs driven.Codecs) *topic.Topic {
	t.Helper()
	tp := topic.NewLogTopic(topic.Params{
		Store: store, TopicName: name, MaxBlockSize: 1 << 20, Codec: codec, Codecs: codecs,
	})
	if err := tp.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return tp
}

// The point of the exercise: a log written through this codec is smaller, and every offset
// still reads back byte for byte.
func TestALogWrittenWithZstdIsSmallerAndStillReadable(t *testing.T) {
	const entries = 500
	codec := newCodec(t, Default)
	codecs := driven.NewCodecs(codec)

	plainStore := memstore.New()
	writeTo(t, newTopic(t, plainStore, "plain", nil, codecs), 0, entries)

	zstdStore := memstore.New()
	compressed := newTopic(t, zstdStore, "compressed", codec, codecs)
	writeTo(t, compressed, 0, entries)

	plainSize := blockBytes(t, plainStore, "plain")
	zstdSize := blockBytes(t, zstdStore, "compressed")
	if zstdSize >= plainSize {
		t.Errorf("zstd wrote %d bytes where no codec wrote %d", zstdSize, plainSize)
	}
	t.Logf("%d entries: %d bytes plain, %d bytes zstd", entries, plainSize, zstdSize)

	for from := 0; from < entries; from++ {
		got := readFrom(t, compressed, domain.Offset(from))
		if len(got) != entries-from {
			t.Fatalf("read from %d gave %d entries, want %d", from, len(got), entries-from)
		}
		for i, entry := range got {
			if entry.Offset != uint64(from+i) {
				t.Fatalf("read from %d: entry %d has offset %d", from, i, entry.Offset)
			}
			if string(entry.Entry) != string(payload(from+i)) {
				t.Fatalf("offset %d came back as %q", entry.Offset, entry.Entry)
			}
		}
	}
}

// A topic written with zstd reloads: recovery walks the frames on checksums, and the index is
// rebuilt from a block it never decodes.
func TestAZstdTopicReloads(t *testing.T) {
	codec := newCodec(t, Default)
	codecs := driven.NewCodecs(codec)
	store := memstore.New()

	first := newTopic(t, store, "t", codec, codecs)
	writeTo(t, first, 0, 40)
	if _, err := first.UpdateIndex(); err != nil {
		t.Fatal(err)
	}

	reloaded := newTopic(t, store, "t", codec, codecs)
	if reloaded.NextOffset != 40 {
		t.Fatalf("reloaded at offset %d, want 40", reloaded.NextOffset)
	}
	writeTo(t, reloaded, 40, 10)
	if got := readFrom(t, reloaded, 0); len(got) != 50 {
		t.Fatalf("read %d entries after reload, want 50", len(got))
	}
}

// Turning compression on does not strand what was written without it, and turning it off
// does not strand what was written with it. The codec byte travels with the frame.
func TestABlockSurvivesTheCodecChangingBothWays(t *testing.T) {
	codec := newCodec(t, Default)
	codecs := driven.NewCodecs(codec)
	store := memstore.New()

	writeTo(t, newTopic(t, store, "t", nil, codecs), 0, 10)
	writeTo(t, newTopic(t, store, "t", codec, codecs), 10, 10)
	// and back to no compression, as an operator turning it off again would
	last := newTopic(t, store, "t", nil, codecs)
	writeTo(t, last, 20, 10)

	got := readFrom(t, last, 0)
	if len(got) != 30 {
		t.Fatalf("read %d entries across three codec settings, want 30", len(got))
	}
	for i, entry := range got {
		if string(entry.Entry) != string(payload(i)) {
			t.Fatalf("offset %d came back as %q", entry.Offset, entry.Entry)
		}
	}
}

// A build that did not wire zstd reads the uncompressed frames of a mixed block and says
// plainly what is missing for the rest. Nothing is treated as damage.
func TestAReaderWithoutZstdReportsTheMissingCodec(t *testing.T) {
	codec := newCodec(t, Default)
	codecs := driven.NewCodecs(codec)
	store := memstore.New()
	writeTo(t, newTopic(t, store, "t", nil, codecs), 0, 5)
	writeTo(t, newTopic(t, store, "t", codec, codecs), 5, 5)

	// the same log, opened by a build carrying no compression adapter at all
	without := newTopic(t, store, "t", nil, driven.NewCodecs())
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	go func() {
		for range logChan {
			wg.Done()
		}
	}()
	err := without.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: 0, BatchSize: 100})
	wg.Wait()

	if err == nil {
		t.Fatal("a frame written with an unwired codec was read anyway")
	}
	if !strings.Contains(err.Error(), "unknown codec") || !strings.Contains(err.Error(), "zstd") {
		t.Errorf("error %q does not say which codec is missing", err)
	}
	// and the topic still loaded and recovered, since neither needs the codec
	if without.NextOffset != 10 {
		t.Errorf("loaded at offset %d, want 10: recovery must not need the codec", without.NextOffset)
	}
}

package zstd

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/topic"
)

// What this measures, and why it lives here.
//
// MaxFrameEntries is the dial between how well a codec can compress, which wants large
// frames, and how little a read has to decode to reach one offset, which wants small ones.
// The defaults were picked to be unsurprising, not because anything measured them. These
// benchmarks produce the three numbers that decision needs: how much smaller the log gets,
// what a write costs, and how many bytes a read has to decode to hand over one entry.
//
// It lives beside the codec because it needs both the core and a real compressor, and a
// driving adapter may not reach a driven one. Storage is memstore, so what is timed is
// framing and compression rather than a filesystem, and memstore cannot sync, so the flush
// policy — a separate dial — stays out of the numbers.
//
// MaxFrameBytes is pinned wide open throughout, so the entry bound is the only thing
// deciding how large a frame gets. One write carries benchBatch entries, well past every
// bound measured: a frame is min(what the client writes, the bound), so a bound only bites
// for a client that batches, and a benchmark that wrote one entry at a time would measure
// nothing but the header.

// benchBatch is the entries in one Write. Larger than every bound below, so the bound is
// what decides the frame size.
const benchBatch = 10000

// benchFrameEntries spans a frame per entry to a frame per write.
var benchFrameEntries = []uint32{1, 10, 100, 1000, 10000}

// payloadShape is what the entries look like. A log of structured events is what compression
// is for; incompressible entries are what it costs when there is nothing to find.
type payloadShape struct {
	name  string
	build func(rnd *rand.Rand, offset int) []byte
}

var benchShapes = []payloadShape{
	{
		name: "json",
		build: func(rnd *rand.Rand, offset int) []byte {
			return []byte(fmt.Sprintf(
				`{"event":"order-placed","id":%d,"customer":"cust-%04d","amount":%d,"currency":"NOK","ts":"2026-09-18T22:%02d:%02dZ"}`,
				offset, rnd.Intn(5000), rnd.Intn(100000), rnd.Intn(60), rnd.Intn(60)))
		},
	},
	{
		name: "random",
		build: func(rnd *rand.Rand, _ int) []byte {
			entry := make([]byte, 110)
			rnd.Read(entry)
			return entry
		},
	},
}

// benchCodecs are the two ends of the choice: store the bytes, or compress them.
var benchCodecs = []string{"none", "zstd"}

// countingCodec is the codec under test with a tally of what passed through it. Bytes
// decoded per entry delivered is read amplification measured rather than reasoned about.
type countingCodec struct {
	inner   driven.Codec
	decoded atomic.Int64
}

func (c *countingCodec) ID() driven.CodecID { return c.inner.ID() }

func (c *countingCodec) Encode(dst, src []byte) ([]byte, error) {
	return c.inner.Encode(dst, src)
}

func (c *countingCodec) Decode(dst, src []byte, plainSize int) ([]byte, error) {
	c.decoded.Add(int64(plainSize))
	return c.inner.Decode(dst, src, plainSize)
}

func benchCodec(tb testing.TB, name string) *countingCodec {
	tb.Helper()
	switch name {
	case "none":
		return &countingCodec{inner: driven.NoCodec{}}
	case "zstd":
		return &countingCodec{inner: newCodec(tb, Default)}
	}
	tb.Fatalf("unknown codec %q", name)
	return nil
}

// benchBatches builds rounds distinct batches, so a codec meets new bytes each time rather
// than the same ones over and over.
func benchBatches(shape payloadShape, rounds int) ([][][]byte, int64) {
	rnd := rand.New(rand.NewSource(11))
	batches := make([][][]byte, rounds)
	var raw int64
	for r := range batches {
		batch := make([][]byte, benchBatch)
		for i := range batch {
			batch[i] = shape.build(rnd, r*benchBatch+i)
			raw = raw + int64(len(batch[i])) + domain.EntryOverhead
		}
		batches[r] = batch
	}
	return batches, raw / int64(rounds)
}

func benchTopic(tb testing.TB, store driven.BlockStore, codec driven.Codec, frameEntries uint32) *topic.Topic {
	tb.Helper()
	tp := topic.NewLogTopic(topic.Params{
		Store:           store,
		TopicName:       "bench",
		MaxBlockSize:    1 << 30,
		MaxFrameEntries: frameEntries,
		MaxFrameBytes:   domain.MaxFrameSize,
		Codec:           codec,
		Codecs:          driven.NewCodecs(codec),
	})
	if err := tp.LoadOrCreate(); err != nil {
		tb.Fatal(err)
	}
	return tp
}

// eachCase runs body over every codec, shape and frame bound.
func eachCase(b *testing.B, body func(b *testing.B, codec string, shape payloadShape, frameEntries uint32)) {
	for _, shape := range benchShapes {
		for _, codec := range benchCodecs {
			for _, frameEntries := range benchFrameEntries {
				b.Run(fmt.Sprintf("%s/%s/frameEntries=%d", shape.name, codec, frameEntries), func(b *testing.B) {
					body(b, codec, shape, frameEntries)
				})
			}
		}
	}
}

// BenchmarkFrameWrite is what a write costs and what it leaves on disk. "ratio" is stored
// bytes over the entry bytes handed in, so lower is smaller; at the "none" codec it is the
// framing overhead alone.
func BenchmarkFrameWrite(b *testing.B) {
	eachCase(b, func(b *testing.B, codecName string, shape payloadShape, frameEntries uint32) {
		const rounds = 8
		batches, rawPerBatch := benchBatches(shape, rounds)
		codec := benchCodec(b, codecName)
		store := memstore.New()
		tp := benchTopic(b, store, codec, frameEntries)

		b.SetBytes(rawPerBatch)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch := batches[i%rounds]
			if err := tp.Write(&batch); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		stored := blockBytes(b, store, "bench")
		b.ReportMetric(float64(stored)/float64(rawPerBatch*int64(b.N)), "ratio")
		b.ReportMetric(float64(stored)/float64(b.N*benchBatch), "storedB/entry")
	})
}

// BenchmarkFrameReadOne is read amplification: what it costs to hand over one entry from a
// spread of offsets. A frame is decoded whole, so "decodedB/read" is roughly the frame size
// however small the entry asked for.
func BenchmarkFrameReadOne(b *testing.B) {
	eachCase(b, func(b *testing.B, codecName string, shape payloadShape, frameEntries uint32) {
		batches, _ := benchBatches(shape, 1)
		codec := benchCodec(b, codecName)
		tp := benchTopic(b, memstore.New(), codec, frameEntries)
		batch := batches[0]
		if err := tp.Write(&batch); err != nil {
			b.Fatal(err)
		}
		finishIndexing(b, tp)

		codec.decoded.Store(0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// a prime stride walks the whole log without repeating
			offset := domain.Offset((i * 7919) % benchBatch)
			if err := readOneEntry(tp, offset); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		b.ReportMetric(float64(codec.decoded.Load())/float64(b.N), "decodedB/read")
	})
}

// BenchmarkFrameReadAll is the other end: a consumer reading the log from the start, where a
// frame is decoded once and every entry in it is wanted.
func BenchmarkFrameReadAll(b *testing.B) {
	eachCase(b, func(b *testing.B, codecName string, shape payloadShape, frameEntries uint32) {
		batches, raw := benchBatches(shape, 1)
		codec := benchCodec(b, codecName)
		tp := benchTopic(b, memstore.New(), codec, frameEntries)
		batch := batches[0]
		if err := tp.Write(&batch); err != nil {
			b.Fatal(err)
		}
		finishIndexing(b, tp)

		b.SetBytes(raw)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got := countEntriesFrom(b, tp, 0)
			if got != benchBatch {
				b.Fatalf("read %d entries, want %d", got, benchBatch)
			}
		}
	})
}

// finishIndexing runs the index to completion, so a read measures a seek through the index
// rather than a scan from the start of the block.
func finishIndexing(tb testing.TB, tp *topic.Topic) {
	tb.Helper()
	for {
		ran, err := tp.UpdateIndex()
		if err != nil {
			tb.Fatal(err)
		}
		if ran {
			return
		}
	}
}

// readOneEntry reads from an offset and stops at the first entry, which is what a consumer
// asking for one record does. What it costs is the frame holding that offset.
func readOneEntry(tp *topic.Topic, from domain.Offset) error {
	logChan := make(chan *[]domain.LogEntry)
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		stopped := false
		for range logChan {
			wg.Done()
			if !stopped {
				stopped = true
				close(cancel)
			}
		}
	}()
	err := tp.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: 1, Cancel: cancel})
	wg.Wait()
	close(logChan)
	<-done
	if errors.Is(err, domain.ErrReadCancelled) {
		return nil
	}
	return err
}

// countEntriesFrom counts the entries a read of the whole log delivers. It counts rather
// than collects, so the benchmark is not dominated by appending to a slice.
func countEntriesFrom(tb testing.TB, tp *topic.Topic, from domain.Offset) int {
	tb.Helper()
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	count := 0
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			count = count + len(*batch)
			wg.Done()
		}
		close(done)
	}()
	err := tp.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: 1000})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		tb.Fatal(err)
	}
	return count
}

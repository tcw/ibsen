package test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	zstdcodec "github.com/tcw/ibsen/adapter/driven/compression/zstd"
	"github.com/tcw/ibsen/adapter/driven/logging/zerologger"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/wiring"
)

// What these measure: an entry's round trip through everything a deployment actually runs —
// a gRPC client, the wire, the server's handler, the core, a frame put through zstd, and a
// file on a real disk with an fsync in front of the acknowledgement.
//
// Everything is at its default. The parameters are left zero, which is what the core reads
// as its own defaults — index sparsity 10, frames of 1000 entries or 1 MiB, FlushEntries 1
// with no interval, so every write is flushed before it is acknowledged — and the codec is
// zstd at its default level, which is what wiring.DefaultCompression means and what a server
// started with no --compression writes. Only the three values the CLI itself passes are
// named below, because zero does not mean a default for any of them.
//
// The stack is assembled here rather than through wiring.IbsenServer, which is the same
// adapters in the same shape: the composition root also takes the single-writer lease,
// installs signal handlers and prints a banner on stdout, and a benchmark whose output is a
// table of numbers can do without all three. What it would have decided for us is the codec,
// so that is pinned against wiring.DefaultCompression instead of copied — if the default
// moves, these benchmarks fail rather than quietly measuring something else.
//
// They are benchmarks rather than tests, so `go test ./...` does not run them:
//
//	go test ./adapter/driver/grpcapi/test/ -run XXX -bench . -benchtime 2s
//
// What they are sensitive to, and it dominates everything else: where TMPDIR points. The
// default flush policy is one fsync per Write, so a run on tmpfs measures the code and a run
// on a disk measures the disk. TMPDIR=/var/tmp, or somewhere on the media a deployment would
// really use, is what makes the write numbers mean anything.

const (
	// benchMaxBlockSizeMB is the --maxBlockSize the CLI defaults to. Zero would not mean a
	// default here: it would roll a block over on every write.
	benchMaxBlockSizeMB = 1000
	// benchTTL is the read TTL the CLI passes, which is how long a tailing read waits for
	// entries that have not arrived, and benchCheckForNewEvery is how often it looks. Every
	// read below stops on completion, so they only bound what a failing benchmark waits for.
	benchTTL              = 30 * time.Second
	benchCheckForNewEvery = 2 * time.Second
	// benchEntrySize is roughly the shape of a logged event: structured, repetitive, the
	// kind of bytes a log is mostly made of and compression is for.
	benchEntrySize = 130
	// benchCorpus is the topic the read benchmarks are run against, written once in setup.
	benchCorpus = 50_000
)

// benchServer is a running server and the count of what it asked the disk to do.
type benchServer struct {
	target string
	syncs  *countingFS
}

// startBenchServer starts a server over a real directory on a free local port, with the
// adapters a deployment gets, and stops it when the benchmark ends. flushEntries and
// flushInterval are the durability policy; both zero is the default, which flushes every
// write before acknowledging it.
func startBenchServer(b *testing.B, flushEntries uint32, flushInterval time.Duration) benchServer {
	b.Helper()
	if wiring.DefaultCompression != "zstd" {
		b.Fatalf("the default compression is %q, and these benchmarks are written for zstd",
			wiring.DefaultCompression)
	}
	codec, err := zstdcodec.New(zstdcodec.Default)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(codec.Close)
	syncs := newCountingFS()
	topicsManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store:         filestore.New(syncs, b.TempDir()),
		MaxBlockSize:  benchMaxBlockSizeMB * 1024 * 1024,
		TTL:           benchTTL,
		Codec:         codec,
		Codecs:        driven.NewCodecs(codec),
		FlushEntries:  flushEntries,
		FlushInterval: flushInterval,
		// the log has nothing to say per entry, and what it would say does not belong in a
		// table of numbers
		Logger: zerologger.New(zerolog.New(io.Discard)),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(topicsManager.Close)
	server := grpcapi.NewUnsecureIbsenGrpcServer(&topicsManager, benchTTL, benchCheckForNewEvery)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := server.StartGRPC(lis); err != nil {
			b.Errorf("server failed: %v", err)
		}
	}()
	target := lis.Addr().String()
	// dialling waits for the server to answer, so nothing below races the start
	client, err := newIbsenClient(target)
	if err != nil {
		b.Fatalf("server did not start: %v", err)
	}
	client.Close()
	b.Cleanup(func() {
		server.Shutdown()
		<-stopped
	})
	return benchServer{target: target, syncs: syncs}
}

// benchClient is one client for the whole benchmark, since connecting is not what is being
// measured. It is what a program holding a connection open does.
func benchClient(b *testing.B) IbsenClient {
	b.Helper()
	return connect(b, startBenchServer(b, 0, 0).target)
}

// connect opens one client against a running benchmark server.
func connect(b *testing.B, target string) IbsenClient {
	b.Helper()
	client, err := newIbsenClient(target)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(client.Close)
	return client
}

// benchEntries builds one Write's worth of entries, once. Building them inside the timed loop
// would measure this function.
func benchEntries(topic string, count int) *grpcapi.InputEntries {
	rnd := rand.New(rand.NewSource(1))
	entries := make([][]byte, count)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf(
			`{"event":"order-placed","id":%d,"customer":"cust-%04d","amount":%d,"currency":"NOK","ts":"2026-09-20T%02d:%02d:%02dZ"}`,
			i, rnd.Intn(5000), rnd.Intn(100000), rnd.Intn(24), rnd.Intn(60), rnd.Intn(60)))
	}
	return &grpcapi.InputEntries{Topic: topic, Entries: entries}
}

// BenchmarkGrpcWrite is a client writing to a topic on disk, varying only how many entries
// share one Write.
//
// That is the dial worth measuring here, because one Write is one append and one flush: at
// the default policy every call costs an fsync whatever it carries, so the entries in a call
// are the entries that share the fsync. Nothing about durability moves with it — every entry
// is on durable media before the call returns, at every size below.
func BenchmarkGrpcWrite(b *testing.B) {
	for _, batch := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("entries=%d", batch), func(b *testing.B) {
			client := benchClient(b)
			entries := benchEntries("bench", batch)
			ctx := context.Background()
			b.SetBytes(int64(batch * benchEntrySize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := client.Client.Write(ctx, entries); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/entry")
		})
	}
}

// BenchmarkGrpcReadAll reads a topic from its first offset to its end, which is what a
// consumer catching up does and what the frame bound's large end is for.
func BenchmarkGrpcReadAll(b *testing.B) {
	for _, batch := range []uint32{100, 1000, 10000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			client := benchClient(b)
			fillTopic(b, client, "bench", benchCorpus)
			b.SetBytes(int64(benchCorpus * benchEntrySize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if read := drainRead(b, client, "bench", 0, batch); read != benchCorpus {
					b.Fatalf("read %d entries, want %d", read, benchCorpus)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchCorpus), "ns/entry")
		})
	}
}

// BenchmarkGrpcReadOne reads a single entry, which is the other kind of reader and the one
// the default frame bound costs: a frame is decoded whole, so one 130-byte entry comes out of
// a frame holding up to a thousand of them, reached through the index.
//
// It reads the last entry of the topic rather than a moving one, because a read has no count:
// it runs from the offset asked for to the end of the log, so a read from the middle would
// stream everything after it and the number would be the cost of hanging up on a server that
// is still sending. At the last offset the stream ends by itself, and what is left in the
// measurement is the seek, one frame decoded, and one entry on the wire.
func BenchmarkGrpcReadOne(b *testing.B) {
	client := benchClient(b)
	fillTopic(b, client, "bench", benchCorpus)
	b.SetBytes(benchEntrySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if read := drainRead(b, client, "bench", benchCorpus-1, 1); read != 1 {
			b.Fatalf("read %d entries from the last offset, want 1", read)
		}
	}
}

// fillTopic writes the corpus the read benchmarks run against, outside the timer.
func fillTopic(b *testing.B, client IbsenClient, topic string, entries int) {
	b.Helper()
	const perWrite = 1000
	ctx := context.Background()
	batch := benchEntries(topic, perWrite)
	for written := 0; written < entries; written += perWrite {
		if _, err := client.Client.Write(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// drainRead reads from offset until the server says the topic is exhausted, and returns how
// many entries came back. Every read here ends on its own: leaving a stream open and hanging
// up would measure a cancelled read rather than a read.
func drainRead(b *testing.B, client IbsenClient, topic string, offset uint64, batchSize uint32) int {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Client.Read(ctx, &grpcapi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        batchSize,
	})
	if err != nil {
		b.Fatal(err)
	}
	read := 0
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return read
		}
		if err != nil {
			b.Fatal(err)
		}
		read += len(in.Entries)
	}
}

// countingFS is the real filesystem with a tally of the syncs made through it. What is
// counted is what a disk was actually asked to do, which is the only honest way to say what
// a flush policy bought: the port cannot see a sync, so this counts at the store's own seam,
// the way the filestore tests do.
type countingFS struct {
	filestore.FS
	syncs atomic.Int64
}

func newCountingFS() *countingFS { return &countingFS{FS: filestore.OS{}} }

func (c *countingFS) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	file, err := c.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: file, fs: c}, nil
}

type countingFile struct {
	filestore.File
	fs *countingFS
}

func (f *countingFile) Sync() error {
	f.fs.syncs.Add(1)
	return f.File.Sync()
}

// benchFlushWriters is how many clients write at once. The flush policy is about writers that
// arrive together — one writer has nobody to wait for — so a benchmark of it with a single
// stream would measure nothing the default does not already do.
const benchFlushWriters = 8

// benchFlushWrite is the entries one client sends per call, small enough that a batch needs
// several writers before it reaches the entry bound.
const benchFlushWrite = 10

// BenchmarkGrpcWriteFlushPolicy measures the pair in §2: --flushEntries, how many entries may
// wait, and --flushIntervalMs, how long a batch may be held back hoping for more.
//
// They are a pair, and the count does nothing on its own: a batch is due the moment its
// interval is zero, so raising the count alone changes not one fsync. Set an interval and the
// count becomes the escape from it — enough entries have arrived, stop waiting. The cases
// below are that claim, in order: the default, the count alone, the interval alone, and the
// two together with the count low enough that eight writers of ten entries reach it first.
//
// What it costs is latency: every writer in a batch waits for the batch. So the number to
// read beside fsyncs/op is ns/op, which here is wall time divided by the writes all eight
// clients made — throughput, not the latency one of them saw.
//
// On tmpfs an fsync is nearly free and the policy has almost nothing to buy. TMPDIR on real
// media is what makes this benchmark say anything.
//
// It used to say that nothing reaching the log through a server could coalesce a flush:
// LogTopicsManager.Write held a per-topic mutex across the whole of Topic.Write, flush
// included, so every case came out at one fsync per write and an interval only added itself
// to each one. With that mutex gone the default policy already coalesces, since writers that
// arrive while a sync runs append behind it and share the next one: eight clients measured
// 0.33 fsyncs/op at 0.78 ms/op on ext4, against 1.00 at 2.03 ms before. An interval halves
// the syncs again, to 0.14 at 5 ms, but is slower than the default here, because eight
// clients that each wait for their own acknowledgement have nothing to send while they are
// held back. It pays where syncs are dearer than on this disk, or writers are more numerous.
func BenchmarkGrpcWriteFlushPolicy(b *testing.B) {
	for _, policy := range []struct {
		name     string
		entries  uint32
		interval time.Duration
	}{
		{name: "default", entries: 0, interval: 0},
		{name: "entries=1000", entries: 1000, interval: 0},
		{name: "entries=1000,interval=1ms", entries: 1000, interval: time.Millisecond},
		{name: "entries=1000,interval=5ms", entries: 1000, interval: 5 * time.Millisecond},
		{name: "entries=40,interval=5ms", entries: 40, interval: 5 * time.Millisecond},
	} {
		b.Run(policy.name, func(b *testing.B) {
			server := startBenchServer(b, policy.entries, policy.interval)
			clients := make([]IbsenClient, benchFlushWriters)
			for i := range clients {
				clients[i] = connect(b, server.target)
			}
			entries := benchEntries("bench", benchFlushWrite)
			ctx := context.Background()
			b.SetBytes(int64(benchFlushWrite * benchEntrySize))
			b.ResetTimer()
			before := server.syncs.syncs.Load()

			var writers sync.WaitGroup
			for i, client := range clients {
				calls := b.N / benchFlushWriters
				if i < b.N%benchFlushWriters {
					calls++
				}
				writers.Add(1)
				go func(client IbsenClient, calls int) {
					defer writers.Done()
					for c := 0; c < calls; c++ {
						if _, err := client.Client.Write(ctx, entries); err != nil {
							b.Error(err)
							return
						}
					}
				}(client, calls)
			}
			writers.Wait()

			b.StopTimer()
			synced := server.syncs.syncs.Load() - before
			b.ReportMetric(float64(synced)/float64(b.N), "fsyncs/op")
			b.ReportMetric(float64(b.N*benchFlushWrite)/float64(max(synced, 1)), "entries/fsync")
		})
	}
}

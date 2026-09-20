package test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
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

// startBenchServer starts a server over a real directory on a free local port, with the
// adapters and the parameters a deployment gets, and stops it when the benchmark ends.
func startBenchServer(b *testing.B) string {
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
	topicsManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store:        filestore.NewOS(b.TempDir()),
		MaxBlockSize: benchMaxBlockSizeMB * 1024 * 1024,
		TTL:          benchTTL,
		Codec:        codec,
		Codecs:       driven.NewCodecs(codec),
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
	return target
}

// benchClient is one client for the whole benchmark, since connecting is not what is being
// measured. It is what a program holding a connection open does.
func benchClient(b *testing.B) IbsenClient {
	b.Helper()
	client, err := newIbsenClient(startBenchServer(b))
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

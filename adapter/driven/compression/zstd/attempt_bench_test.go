package zstd

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/topic"
)

// BenchmarkCompressionAttempt is what an attempt to compress a frame costs when it is thrown
// away, which is what --compressMinBytes and the adaptive skip exist to save. It frames one
// payload of each size through zstd and through no codec at all, and reports whether zstd's
// result was kept ("kept" is 1 when it paid) and the stored ratio. The difference in ns/op
// between zstd and none for a payload zstd did not keep is pure waste.
//
//	go test ./adapter/driven/compression/zstd/ -run XXX -bench BenchmarkCompressionAttempt
func BenchmarkCompressionAttempt(b *testing.B) {
	rnd := rand.New(rand.NewSource(3))
	jsonEntries := func(size int) []byte {
		var out []byte
		for i := 0; len(out) < size; i++ {
			out = domain.AppendEntry(out, benchShapes[0].build(rnd, i), domain.Offset(i))
		}
		return out[:size]
	}
	random := func(size int) []byte {
		out := make([]byte, size)
		rnd.Read(out)
		return out
	}
	gzipped := func(size int) []byte {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		for buf.Len() < size {
			_, _ = w.Write(jsonEntries(4096))
			_ = w.Flush()
		}
		_ = w.Close()
		return buf.Bytes()[:size]
	}
	shapes := []struct {
		name string
		make func(int) []byte
	}{{"json", jsonEntries}, {"random", random}, {"gzipped", gzipped}}
	zstd := newCodec(b, Default)
	codecs := []struct {
		name  string
		codec driven.Codec
	}{{"none", driven.NoCodec{}}, {"zstd", zstd}}

	for _, shape := range shapes {
		for _, size := range []int{64, 256, 512, 1024, 2048, 4096, 16384} {
			payload := shape.make(size)
			for _, c := range codecs {
				b.Run(fmt.Sprintf("%s/%dB/%s", shape.name, size, c.name), func(b *testing.B) {
					b.SetBytes(int64(size))
					var frame []byte
					var err error
					for i := 0; i < b.N; i++ {
						frame, err = logfmt.AppendFrame(frame[:0], c.codec, 0, 1, payload)
						if err != nil {
							b.Fatal(err)
						}
					}
					kept := 0.0
					if driven.CodecID(frame[8]) != driven.CodecNone {
						kept = 1
					}
					b.ReportMetric(kept, "kept")
					b.ReportMetric(float64(len(frame)-domain.FrameHeaderSize)/float64(size), "ratio")
				})
			}
		}
	}
}

// BenchmarkCompressionSkip is what --compressMinBytes and --adaptiveCompression save, through a
// real topic writing 10000-entry batches: CPU on entries that never compress, and nothing of
// the ratio on entries that do. "off" tries every frame, as before either existed.
//
//	go test ./adapter/driven/compression/zstd/ -run XXX -bench BenchmarkCompressionSkip
func BenchmarkCompressionSkip(b *testing.B) {
	type setting struct {
		name     string
		minBytes int
		adaptive bool
	}
	settings := []setting{{"off", 0, false}, {"on", 128, true}}
	for _, shape := range benchShapes {
		for _, frameEntries := range []uint32{1, 10, 100} {
			for _, s := range settings {
				b.Run(fmt.Sprintf("%s/frameEntries=%d/skip=%s", shape.name, frameEntries, s.name), func(b *testing.B) {
					codec := newCodec(b, Default)
					store := memstore.New()
					tp := topic.NewLogTopic(topic.Params{
						Store: store, TopicName: "bench", MaxBlockSize: 1 << 30,
						MaxFrameEntries: frameEntries, MaxFrameBytes: domain.MaxFrameSize,
						Codec: codec, Codecs: driven.NewCodecs(codec),
						CompressMinBytes: s.minBytes, AdaptiveCompression: s.adaptive,
					})
					if err := tp.LoadOrCreate(); err != nil {
						b.Fatal(err)
					}
					defer tp.Close()
					const rounds = 4
					batches, raw := benchBatches(shape, rounds)
					b.SetBytes(raw)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := batches[i%rounds]
						if err := tp.Write(&batch); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(blockBytes(b, store, "bench"))/float64(raw*int64(b.N)), "ratio")
				})
			}
		}
	}
}

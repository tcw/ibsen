package zstd

import (
	"fmt"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/topic"
)

// BenchmarkDictionary is what a trained dictionary buys on the ~130-byte JSON events of
// BenchmarkFrameWrite, at the small frames where zstd alone has least to work with. Each case
// writes a warm-up batch, waits for the topic to train its dictionary (when it is on), and then
// measures writes of fresh batches: "ratio" is stored bytes over entry bytes, as in §13.2 of
// ARCHITECTURE.md, and "readMB/s" is a sequential read of what was measured.
//
//	go test ./adapter/driven/compression/zstd/ -run XXX -bench BenchmarkDictionary
func BenchmarkDictionary(b *testing.B) {
	for _, frameEntries := range []uint32{1, 10, 100} {
		for _, dictionaries := range []bool{false, true} {
			name := fmt.Sprintf("frameEntries=%d/dictionary=%v", frameEntries, dictionaries)
			b.Run(name, func(b *testing.B) {
				codec := newCodec(b, Default)
				store := memstore.New()
				tp := topic.NewLogTopic(topic.Params{
					Store: store, TopicName: "bench", MaxBlockSize: 1 << 30,
					MaxFrameEntries: frameEntries, MaxFrameBytes: domain.MaxFrameSize,
					Codec: codec, Codecs: driven.NewCodecs(codec),
					Dictionaries: dictionaries, DictTrainBytes: 256 << 10,
				})
				if err := tp.LoadOrCreate(); err != nil {
					b.Fatal(err)
				}
				defer tp.Close()
				batches, raw := benchBatches(benchShapes[0], b.N+1)
				if err := tp.Write(&batches[0]); err != nil {
					b.Fatal(err)
				}
				if dictionaries {
					// training runs off the write path; where a dictionary does not pay at
					// this frame size none is kept, and the case measures that
					deadline := time.Now().Add(5 * time.Second)
					for time.Now().Before(deadline) {
						if dicts, _ := store.List("bench", driven.Dict); len(dicts) > 0 {
							break
						}
						time.Sleep(time.Millisecond)
					}
					dicts, _ := store.List("bench", driven.Dict)
					b.ReportMetric(float64(len(dicts)), "dictionaries")
					// one more batch, so the frames measured are all written with it
					if err := tp.Write(&batches[0]); err != nil {
						b.Fatal(err)
					}
				}
				before := blockBytes(b, store, "bench")
				start := tp.NextOffset
				b.ResetTimer()
				for i := 1; i <= b.N; i++ {
					if err := tp.Write(&batches[i]); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				stored := blockBytes(b, store, "bench") - before
				b.ReportMetric(float64(stored)/float64(raw*int64(b.N)), "ratio")

				began := time.Now()
				got := readCount(b, tp, start)
				if got != b.N*benchBatch {
					b.Fatalf("read %d entries, want %d", got, b.N*benchBatch)
				}
				b.ReportMetric(float64(raw*int64(b.N))/time.Since(began).Seconds()/1e6, "readMB/s")
			})
		}
	}
}

func readCount(tb testing.TB, tp *topic.Topic, from domain.Offset) int {
	tb.Helper()
	got, err := readSome(tp, from)
	if err != nil {
		tb.Fatal(err)
	}
	return got
}

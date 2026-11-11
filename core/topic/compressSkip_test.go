package topic

import (
	"bytes"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// zeroRuns is a counting codec that run-length encodes zero bytes: entries full of zeros
// shrink, random ones grow and are stored plain. It counts every Encode, which is the attempt
// the threshold and the adaptive skip exist to save.
type zeroRuns struct{ encodes atomic.Int64 }

func (z *zeroRuns) ID() driven.CodecID { return 99 }

// zeroRunsHeader is what the codec spends on every frame, as a real format's header does, so
// that a frame with little to find in it comes back larger.
var zeroRunsHeader = []byte("zero-runs-codec!")

func (z *zeroRuns) Encode(dst, src []byte) ([]byte, error) {
	z.encodes.Add(1)
	dst = append(dst, zeroRunsHeader...)
	for i := 0; i < len(src); {
		if src[i] != 0 {
			dst = append(dst, src[i])
			i++
			continue
		}
		run := 0
		for i < len(src) && src[i] == 0 && run < 255 {
			run++
			i++
		}
		dst = append(dst, 0, byte(run))
	}
	return dst, nil
}

func (z *zeroRuns) Decode(dst, src []byte, plainSize int) ([]byte, error) {
	if !bytes.HasPrefix(src, zeroRunsHeader) {
		return nil, errors.New("not a zero-runs frame")
	}
	src = src[len(zeroRunsHeader):]
	for i := 0; i < len(src); i++ {
		if src[i] != 0 {
			dst = append(dst, src[i])
			continue
		}
		if i+1 >= len(src) {
			return nil, errors.New("truncated run")
		}
		dst = append(dst, make([]byte, src[i+1])...)
		i++
	}
	if len(dst) != plainSize {
		return nil, errors.New("wrong size")
	}
	return dst, nil
}

func skipTopic(t *testing.T, codec *zeroRuns, minBytes int, adaptive bool) *Topic {
	t.Helper()
	topic := NewLogTopic(Params{Store: memstore.New(), TopicName: "t", MaxBlockSize: 1 << 30,
		Codec: codec, Codecs: driven.NewCodecs(codec), MaxFrameEntries: 1,
		CompressMinBytes: minBytes, AdaptiveCompression: adaptive})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { topic.Close() })
	return topic
}

var rnd = rand.New(rand.NewSource(5))

func zeros(n int) []byte { return make([]byte, n) }

func noise(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(1 + rnd.Intn(255)) // no zeros: the codec can only make it larger
	}
	return out
}

func writeEntry(t *testing.T, topic *Topic, entry []byte) {
	t.Helper()
	entries := [][]byte{entry}
	if err := topic.Write(&entries); err != nil {
		t.Fatal(err)
	}
}

func TestCompressSkip_belowTheThresholdNoCodecIsCalled(t *testing.T) {
	codec := &zeroRuns{}
	topic := skipTopic(t, codec, 200, false)
	// a frame is the entry plus its 20 bytes of checksum, size and offset
	writeEntry(t, topic, zeros(100))
	if n := codec.encodes.Load(); n != 0 {
		t.Fatalf("a 120-byte frame was offered to the codec %d times, below a threshold of 200", n)
	}
	writeEntry(t, topic, zeros(500))
	if n := codec.encodes.Load(); n != 1 {
		t.Fatalf("a 520-byte frame was offered %d times, want once", n)
	}
	// zero tries everything
	every := &zeroRuns{}
	writeEntry(t, skipTopic(t, every, 0, false), zeros(1))
	if every.encodes.Load() != 1 {
		t.Fatal("with no threshold a small frame was not offered")
	}
}

func TestCompressSkip_adaptiveStopsAfterMissesAndProbes(t *testing.T) {
	codec := &zeroRuns{}
	topic := skipTopic(t, codec, 0, true)
	for i := 0; i < AdaptiveMisses; i++ {
		writeEntry(t, topic, noise(200))
	}
	if n := codec.encodes.Load(); n != AdaptiveMisses {
		t.Fatalf("%d attempts before the skip, want %d", n, AdaptiveMisses)
	}
	// now it skips, and probes every AdaptiveProbeEvery frames
	for i := 0; i < 3*AdaptiveProbeEvery; i++ {
		writeEntry(t, topic, noise(200))
	}
	if n := codec.encodes.Load(); n != AdaptiveMisses+3 {
		t.Fatalf("%d attempts after %d skipped frames, want %d: one probe every %d", n, 3*AdaptiveProbeEvery, AdaptiveMisses+3, AdaptiveProbeEvery)
	}
	// the data becomes compressible: the next probe pays, and every frame is tried again
	for i := 0; i < AdaptiveProbeEvery; i++ {
		writeEntry(t, topic, zeros(200))
	}
	before := codec.encodes.Load()
	for i := 0; i < 10; i++ {
		writeEntry(t, topic, zeros(200))
	}
	if n := codec.encodes.Load() - before; n != 10 {
		t.Fatalf("after a probe paid, %d of 10 frames were tried, want all", n)
	}
}

func TestCompressSkip_offTriesEveryFrame(t *testing.T) {
	codec := &zeroRuns{}
	topic := skipTopic(t, codec, 0, false)
	for i := 0; i < 100; i++ {
		writeEntry(t, topic, noise(200))
	}
	if n := codec.encodes.Load(); n != 100 {
		t.Fatalf("%d attempts, want every frame", n)
	}
}

// Frames tried, skipped and kept share a block and read back as written.
func TestCompressSkip_mixedBlocksReadBack(t *testing.T) {
	codec := &zeroRuns{}
	topic := skipTopic(t, codec, 64, true)
	var written [][]byte
	for i := 0; i < 300; i++ {
		var entry []byte
		switch {
		case i%7 == 0:
			entry = zeros(10) // below the threshold
		case i < 150:
			entry = noise(100 + i)
		default:
			entry = zeros(100 + i)
		}
		written = append(written, entry)
		writeEntry(t, topic, entry)
	}
	got, err := readAllFrom(topic, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(written) {
		t.Fatalf("read %d entries, want %d", len(got), len(written))
	}
	for i, entry := range got {
		if !bytes.Equal(entry.Entry, written[i]) || entry.Offset != uint64(i) {
			t.Fatalf("offset %d read back wrong", i)
		}
	}
	// and the stored frames say both: some compressed, some plain
	blocks, _ := topic.Store.List("t", driven.Log)
	if blocks[0].Size >= int64(len(written))*domain.EntryOverhead+sumLen(written) {
		t.Error("nothing was compressed")
	}
}

func sumLen(entries [][]byte) int64 {
	var n int64
	for _, e := range entries {
		n += int64(len(e))
	}
	return n
}

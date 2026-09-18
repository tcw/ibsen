package topic

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// frameHeaders reads the header of every frame in a log block, which is how these tests ask
// what a write actually laid down.
func frameHeaders(t *testing.T, store driven.BlockStore, ref driven.BlockRef) []domain.FrameHeader {
	t.Helper()
	reader := bufio.NewReader(bytes.NewReader(blockBytes(t, store, ref)))
	var headers []domain.FrameHeader
	for {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err != nil {
			return headers
		}
		if err = domain.SkipFramePayload(reader, header); err != nil {
			t.Fatal(err)
		}
		headers = append(headers, header)
	}
}

func headBlockFrames(t *testing.T, topic *Topic) []domain.FrameHeader {
	t.Helper()
	head, ok := topic.logBlockHead()
	if !ok {
		t.Fatal("topic has no head block")
	}
	return frameHeaders(t, topic.Store, topic.logRef(head))
}

// countingStore reports how many appends reached the store.
type countingStore struct {
	driven.BlockStore
	appends atomic.Int64
}

func (c *countingStore) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	if ref.Kind == driven.Log {
		c.appends.Add(1)
	}
	return c.BlockStore.Append(ref, data)
}

// A write that fits inside the bounds is one frame, which is the ordinary case.
func TestASmallWriteIsOneFrame(t *testing.T) {
	store := memstore.New()
	topic := newTestTopic(t, store, 1<<20)

	writeEntries(t, topic, 0, 5)

	headers := headBlockFrames(t, topic)
	if len(headers) != 1 {
		t.Fatalf("a write of 5 entries made %d frames, want 1", len(headers))
	}
	if headers[0].FirstOffset != 0 || headers[0].EntryCount != 5 {
		t.Errorf("frame covers %d..%d, want 0..4", headers[0].FirstOffset, headers[0].EndOffset()-1)
	}
}

// A frame is decoded whole and an index pair points at its start, so a frame is bounded and
// a write larger than the bound becomes several. Without this a client writing a million
// entries at once would leave one frame that a read of any offset in it has to decode in
// full, and that the index could offer a single pair for.
func TestALargeWriteBecomesSeveralFrames(t *testing.T) {
	store := memstore.New()
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, MaxFrameEntries: 4})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}

	writeEntries(t, topic, 0, 10)

	headers := headBlockFrames(t, topic)
	if len(headers) != 3 {
		t.Fatalf("10 entries at 4 to a frame made %d frames, want 3", len(headers))
	}
	for i, want := range []struct {
		first domain.Offset
		count uint32
	}{{0, 4}, {4, 4}, {8, 2}} {
		if headers[i].FirstOffset != want.first || headers[i].EntryCount != want.count {
			t.Errorf("frame %d covers %d entries from %d, want %d from %d",
				i, headers[i].EntryCount, headers[i].FirstOffset, want.count, want.first)
		}
	}
	assertReadsFromEveryOffset(t, topic, 10)
}

// The byte bound splits a write too, since it is the one that decides how much a read has to
// hold in memory to reach an offset.
func TestFramesAreBoundedByBytesAsWellAsEntries(t *testing.T) {
	store := memstore.New()
	// room for two of these entries, not three
	payload := []byte("fixed-size")
	oneEntry := domain.EntryOverhead + len(payload)
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, MaxFrameBytes: 2 * oneEntry})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}

	entries := [][]byte{payload, payload, payload, payload, payload}
	if err := topic.Write(&entries); err != nil {
		t.Fatal(err)
	}

	headers := headBlockFrames(t, topic)
	if len(headers) != 3 {
		t.Fatalf("5 entries at 2 to a frame made %d frames, want 3", len(headers))
	}
	for i, want := range []uint32{2, 2, 1} {
		if headers[i].EntryCount != want {
			t.Errorf("frame %d holds %d entries, want %d", i, headers[i].EntryCount, want)
		}
	}
	got, err := readAllFrom(topic, 0, 10)
	if err != nil || len(got) != 5 {
		t.Fatalf("read back %d entries with err=%v, want 5", len(got), err)
	}
}

// One entry larger than the bound still gets written: it goes in a frame of its own rather
// than being refused for being what it is.
func TestAnEntryLargerThanTheFrameBoundGetsAFrameOfItsOwn(t *testing.T) {
	store := memstore.New()
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, MaxFrameBytes: 8})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}

	big := bytes.Repeat([]byte("x"), 4096)
	entries := [][]byte{big, []byte("small")}
	if err := topic.Write(&entries); err != nil {
		t.Fatal(err)
	}

	headers := headBlockFrames(t, topic)
	if len(headers) != 2 {
		t.Fatalf("made %d frames, want one for each entry", len(headers))
	}
	got, err := readAllFrom(topic, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !bytes.Equal(got[0].Entry, big) || string(got[1].Entry) != "small" {
		t.Errorf("read back %d entries, want the oversized one and the small one", len(got))
	}
}

// Splitting a write into frames must not split the write itself. The store sees one append,
// so a crash still leaves either all of a write or none of it, and a flush still never lands
// inside a frame.
func TestAWriteIsOneAppendHoweverManyFramesItMakes(t *testing.T) {
	store := &countingStore{BlockStore: memstore.New()}
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, MaxFrameEntries: 2})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}

	writeEntries(t, topic, 0, 9)

	if got := store.appends.Load(); got != 1 {
		t.Errorf("a write of 9 entries reached the store as %d appends, want 1", got)
	}
	if frames := len(headBlockFrames(t, topic)); frames != 5 {
		t.Errorf("that one append carried %d frames, want 5", frames)
	}
}

// compressiblePayload is a run of one byte, which rleCodec shrinks. A codec that does not
// shrink its input is thrown away frame by frame, so a test about which codec a frame names
// has to give the codec something it can do.
func compressiblePayload(offset int) []byte {
	return []byte(strings.Repeat(string(rune('a'+offset%26)), 300))
}

func writeCompressible(t *testing.T, tp *Topic, from, count int) {
	t.Helper()
	batch := make([][]byte, count)
	for i := range batch {
		batch[i] = compressiblePayload(from + i)
	}
	if err := tp.Write(&batch); err != nil {
		t.Fatal(err)
	}
	tp.indexWg.Wait()
}

// A frame says which codec wrote it, so a block may hold frames of several codecs and a read
// picks the right one per frame. That is what makes changing the codec safe: nothing already
// written has to be rewritten.
func TestABlockCanHoldFramesOfSeveralCodecs(t *testing.T) {
	store := memstore.New()
	codecs := driven.NewCodecs(rleCodec{})
	params := Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, Codecs: codecs}

	plain := NewLogTopic(params)
	if err := plain.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeCompressible(t, plain, 0, 3)

	// the same topic, reopened by a build that now writes with another codec
	compressed := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20,
		Codec: rleCodec{}, Codecs: codecs})
	if err := compressed.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeCompressible(t, compressed, 3, 3)

	headers := headBlockFrames(t, compressed)
	if len(headers) != 2 {
		t.Fatalf("got %d frames, want one per write", len(headers))
	}
	if headers[0].Codec != uint8(driven.CodecNone) || headers[1].Codec != uint8(rleCodec{}.ID()) {
		t.Errorf("frames name codecs %d and %d, want %d then %d",
			headers[0].Codec, headers[1].Codec, driven.CodecNone, rleCodec{}.ID())
	}
	// both frames read back whole, from the topic that wrote the second and from a reload
	for name, tp := range map[string]*Topic{"as written": compressed, "reloaded": newTestTopicWith(t, params)} {
		got, err := readAllFrom(tp, 0, 10)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != 6 {
			t.Fatalf("%s: read %d entries, want 6", name, len(got))
		}
		for i, entry := range got {
			if !bytes.Equal(entry.Entry, compressiblePayload(i)) {
				t.Errorf("%s: offset %d came back changed", name, entry.Offset)
			}
		}
	}
}

// The other half of the same rule: a codec whose output is not smaller is not recorded at
// all, and the frame keeps the plain bytes. expandingCodec is the worst case on purpose —
// what a real compressor does to a frame too small to find anything in.
func TestAFrameKeepsThePlainBytesWhenCompressionDoesNotPay(t *testing.T) {
	store := memstore.New()
	topic := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20,
		Codec: expandingCodec{}, Codecs: driven.NewCodecs(expandingCodec{})})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}

	writeEntries(t, topic, 0, 5)

	frames := headBlockFrames(t, topic)
	if len(frames) == 0 {
		t.Fatal("no frames were written")
	}
	for i, header := range frames {
		if header.Codec != uint8(driven.CodecNone) {
			t.Errorf("frame %d names codec %d, want the plain bytes", i, header.Codec)
		}
		if header.StoredSize != header.PlainSize {
			t.Errorf("frame %d stored %d bytes for %d plain", i, header.StoredSize, header.PlainSize)
		}
	}
	assertReadsFromEveryOffset(t, topic, 5)
}

// And the whole point, at the level someone notices it: a topic is never larger for having a
// codec wired, whatever that codec makes of the entries. One entry to a write is the shape
// that gives a compressor the least to work with, and the shape the default durability
// policy produces for a client that does not batch.
func TestACodecNeverMakesATopicLarger(t *testing.T) {
	for _, codec := range []driven.Codec{rleCodec{}, expandingCodec{}} {
		t.Run(codec.ID().String(), func(t *testing.T) {
			plainStore, codecStore := memstore.New(), memstore.New()
			newWith := func(store driven.BlockStore, codec driven.Codec) *Topic {
				tp := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20,
					Codec: codec, Codecs: driven.NewCodecs(rleCodec{}, expandingCodec{})})
				if err := tp.LoadOrCreate(); err != nil {
					t.Fatal(err)
				}
				return tp
			}
			plain := newWith(plainStore, nil)
			withCodec := newWith(codecStore, codec)

			for i := 0; i < 20; i++ {
				writeEntries(t, plain, i, 1)
				writeEntries(t, withCodec, i, 1)
			}

			plainSize := blockSize(t, plainStore, plain.logRef(0))
			codecSize := blockSize(t, codecStore, withCodec.logRef(0))
			if codecSize > plainSize {
				t.Errorf("the topic is %d bytes with the %s codec and %d without",
					codecSize, codec.ID(), plainSize)
			}
			assertReadsFromEveryOffset(t, withCodec, 20)
		})
	}
}

// expandingCodec always makes its input larger, which is the worst a codec can do and the
// case the fallback exists for.
type expandingCodec struct{}

func (expandingCodec) ID() driven.CodecID { return driven.CodecID(43) }

func (expandingCodec) Encode(dst, src []byte) ([]byte, error) {
	dst = append(dst, src...)
	return append(dst, bytes.Repeat([]byte{0xff}, 64)...), nil
}

func (expandingCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src[:len(src)-64]...), nil
}

func newTestTopicWith(t *testing.T, params Params) *Topic {
	t.Helper()
	topic := NewLogTopic(params)
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return topic
}

// rleCodec collapses runs of a repeated byte into a (count, byte) pair. It shrinks input
// with long runs and grows input without them, so one codec exercises both sides of a frame
// keeping compression only when it paid.
type rleCodec struct{}

func (rleCodec) ID() driven.CodecID { return driven.CodecID(42) }

func (rleCodec) Encode(dst, src []byte) ([]byte, error) {
	for i := 0; i < len(src); {
		run := 1
		for i+run < len(src) && src[i+run] == src[i] && run < 255 {
			run++
		}
		dst = append(dst, byte(run), src[i])
		i = i + run
	}
	return dst, nil
}

func (rleCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	if len(src)%2 != 0 {
		return nil, errors.New("rle payload is not whole pairs")
	}
	for i := 0; i < len(src); i = i + 2 {
		for n := 0; n < int(src[i]); n++ {
			dst = append(dst, src[i+1])
		}
	}
	return dst, nil
}

// Zero means the default, the way every other knob on a topic works.
func TestZeroFrameBoundsMeanTheDefaults(t *testing.T) {
	topic := NewLogTopic(Params{Store: memstore.New(), TopicName: "t", MaxBlockSize: 1 << 20})

	if topic.MaxFrameEntries != DefaultMaxFrameEntries {
		t.Errorf("MaxFrameEntries is %d for a caller that chose none, want %d",
			topic.MaxFrameEntries, DefaultMaxFrameEntries)
	}
	if topic.MaxFrameBytes != DefaultMaxFrameBytes {
		t.Errorf("MaxFrameBytes is %d for a caller that chose none, want %d",
			topic.MaxFrameBytes, DefaultMaxFrameBytes)
	}
	if topic.Codec.ID() != driven.CodecNone {
		t.Errorf("a topic given no codec writes with %s, want none", topic.Codec.ID())
	}
}

package topic

import (
	"bufio"
	"bytes"
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

// A frame says which codec wrote it, so a block may hold frames of several codecs and a read
// picks the right one per frame. That is what makes changing the codec safe: nothing already
// written has to be rewritten.
func TestABlockCanHoldFramesOfSeveralCodecs(t *testing.T) {
	store := memstore.New()
	codecs := driven.NewCodecs(reverseCodec{})
	params := Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, Codecs: codecs}

	plain := NewLogTopic(params)
	if err := plain.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeEntries(t, plain, 0, 3)

	// the same topic, reopened by a build that now writes with another codec
	reversed := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20,
		Codec: reverseCodec{}, Codecs: codecs})
	if err := reversed.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeEntries(t, reversed, 3, 3)

	headers := headBlockFrames(t, reversed)
	if len(headers) != 2 {
		t.Fatalf("got %d frames, want one per write", len(headers))
	}
	if headers[0].Codec != uint8(driven.CodecNone) || headers[1].Codec != uint8(reverseCodec{}.ID()) {
		t.Errorf("frames name codecs %d and %d, want %d then %d",
			headers[0].Codec, headers[1].Codec, driven.CodecNone, reverseCodec{}.ID())
	}
	assertReadsFromEveryOffset(t, reversed, 6)
	// and a reload reads both back the same way
	assertReadsFromEveryOffset(t, newTestTopicWith(t, params), 6)
}

func newTestTopicWith(t *testing.T, params Params) *Topic {
	t.Helper()
	topic := NewLogTopic(params)
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return topic
}

// reverseCodec changes the bytes it is given, so a frame written with it is unreadable
// without it. It is its own inverse.
type reverseCodec struct{}

func (reverseCodec) ID() driven.CodecID { return driven.CodecID(42) }

func (reverseCodec) Encode(dst, src []byte) ([]byte, error) {
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst, nil
}

func (r reverseCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return r.Encode(dst, src)
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

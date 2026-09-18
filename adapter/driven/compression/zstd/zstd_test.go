package zstd

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/tcw/ibsen/core/port/driven"
)

func newCodec(tb testing.TB, level Level) *Codec {
	tb.Helper()
	codec, err := New(level)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(codec.Close)
	return codec
}

func TestRoundTripAtEveryLevel(t *testing.T) {
	// log-shaped: repetitive enough that compressing it is worth doing
	var plain []byte
	for i := 0; i < 500; i++ {
		plain = append(plain, []byte("{\"event\":\"order-placed\",\"id\":12345,\"amount\":99}")...)
	}
	for _, level := range []Level{"", Fastest, Default, Better, Best} {
		t.Run(string(level), func(t *testing.T) {
			codec := newCodec(t, level)

			stored, err := codec.Encode(nil, plain)
			if err != nil {
				t.Fatal(err)
			}
			if len(stored) >= len(plain) {
				t.Errorf("compressed %d bytes to %d, which is no compression at all", len(plain), len(stored))
			}
			got, err := codec.Decode(nil, stored, len(plain))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plain) {
				t.Error("the payload did not survive the round trip")
			}
		})
	}
}

// The port says Encode appends to dst, because a frame is built by appending a payload to a
// header.
func TestEncodeAndDecodeAppendToDst(t *testing.T) {
	codec := newCodec(t, Default)
	plain := bytes.Repeat([]byte("entry"), 100)

	stored, err := codec.Encode([]byte("header"), plain)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(stored, []byte("header")) {
		t.Fatal("Encode replaced dst instead of appending to it")
	}
	got, err := codec.Decode([]byte("before"), stored[len("header"):], len(plain))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append([]byte("before"), plain...)) {
		t.Error("Decode replaced dst instead of appending to it")
	}
}

// The port says Encode must not retain or modify its input, since the caller reuses that
// buffer for the next frame.
func TestEncodeDoesNotTouchItsInput(t *testing.T) {
	codec := newCodec(t, Default)
	plain := bytes.Repeat([]byte("entry"), 100)
	want := append([]byte(nil), plain...)

	if _, err := codec.Encode(nil, plain); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(plain, want) {
		t.Error("Encode changed its input")
	}
}

func TestIDIsTheByteAFrameCarries(t *testing.T) {
	if got := newCodec(t, Default).ID(); got != driven.CodecZstd {
		t.Errorf("ID is %s, want zstd", got)
	}
}

// An empty payload is a frame holding no entries, which a write of nothing produces.
func TestEmptyPayloadRoundTrips(t *testing.T) {
	codec := newCodec(t, Default)

	stored, err := codec.Encode(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(nil, stored, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("an empty payload came back as %d bytes", len(got))
	}
}

// A level that came from a flag someone typed is refused rather than quietly treated as the
// default, which would compress a log differently from what was asked for.
func TestAnUnknownLevelIsRefused(t *testing.T) {
	codec, err := New("turbo")

	if err == nil {
		codec.Close()
		t.Fatal("an unknown level was accepted")
	}
	if !strings.Contains(err.Error(), "turbo") {
		t.Errorf("error %q does not name the level it refused", err)
	}
}

// One codec serves every topic, so it has to hold up under concurrent frames.
func TestOneCodecServesConcurrentFrames(t *testing.T) {
	codec := newCodec(t, Default)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			plain := bytes.Repeat([]byte{byte(n)}, 4096)
			for j := 0; j < 20; j++ {
				stored, err := codec.Encode(nil, plain)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := codec.Decode(nil, stored, len(plain))
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(got, plain) {
					t.Error("a payload came back as another goroutine's")
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// A frame claiming to decode to more than the decoder will hold is refused before anything
// is allocated for it, not after.
func TestAFrameTooLargeToHoldIsRefusedBeforeDecoding(t *testing.T) {
	codec := newCodec(t, Default)

	_, err := codec.Decode(nil, []byte("whatever"), maxDecoderMemory+1)

	if err == nil {
		t.Fatal("a frame larger than the decoder limit was accepted")
	}
}

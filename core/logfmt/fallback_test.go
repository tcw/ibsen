package logfmt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// headerOf reads back the header of a frame these tests just built.
func headerOf(t *testing.T, frame []byte) domain.FrameHeader {
	t.Helper()
	header, err := domain.ReadFrameHeader(bytes.NewReader(frame), domain.MaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	return header
}

// A codec is taken up on its offer only when what comes back is smaller. Compression that
// did not pay is thrown away, and the header says the frame holds plain bytes.
func TestEncodeFrame_keepsCompressionOnlyWhenItPays(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		wantCodec driven.CodecID
	}{
		// long runs: the codec shrinks this, so the frame is stored compressed
		{name: "compression paid", payload: strings.Repeat("a", 300), wantCodec: rleCodec{}.ID()},
		// no runs at all: this codec doubles it, so the frame keeps the plain bytes
		{name: "compression did not pay", payload: "abcdefghijklmnopqrstuvwxyz", wantCodec: driven.CodecNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries := entriesFrom(0, test.payload)

			frame, err := EncodeFrame(rleCodec{}, 0, 1, entries)
			if err != nil {
				t.Fatal(err)
			}

			header := headerOf(t, frame)
			if driven.CodecID(header.Codec) != test.wantCodec {
				t.Errorf("frame names codec %s, want %s", driven.CodecID(header.Codec), test.wantCodec)
			}
			if header.PlainSize != uint32(len(entries)) {
				t.Errorf("plain size is %d, want the %d entry bytes handed in", header.PlainSize, len(entries))
			}
			// whichever way it went, the frame is never larger than the entries it holds
			if header.StoredSize > header.PlainSize {
				t.Errorf("stored %d bytes for %d of entries", header.StoredSize, header.PlainSize)
			}
		})
	}
}

// The guarantee, stated as a property: whatever a codec does, a frame never stores more
// bytes than the entries handed to it. Without this, turning compression on could make a
// topic larger, which is the opposite of the point.
func TestEncodeFrame_neverStoresMoreThanThePlainEntries(t *testing.T) {
	for _, payload := range []string{
		"",
		"a",
		"abcdefghijklmnopqrstuvwxyz",
		strings.Repeat("ab", 200),
		strings.Repeat("z", 5000),
	} {
		for _, codec := range []driven.Codec{driven.NoCodec{}, rleCodec{}, expandingCodec{}} {
			entries := entriesFrom(0, payload)

			frame, err := EncodeFrame(codec, 0, 1, entries)
			if err != nil {
				t.Fatal(err)
			}

			header := headerOf(t, frame)
			if header.StoredSize > uint32(len(entries)) {
				t.Errorf("codec %s on a %d byte payload stored %d bytes for %d of entries",
					codec.ID(), len(payload), header.StoredSize, len(entries))
			}
		}
	}
}

// A frame that fell back is readable by a build carrying no codec at all, which is the
// second thing the fallback buys: the smallest frames, the ones a codec could do nothing
// with, stop depending on that codec being wired.
func TestEncodeFrame_aFrameThatFellBackNeedsNoCodec(t *testing.T) {
	entries := entriesFrom(0, "abcdefghijklmnopqrstuvwxyz")
	frame, err := EncodeFrame(rleCodec{}, 0, 1, entries)
	if err != nil {
		t.Fatal(err)
	}
	header := headerOf(t, frame)

	got, err := DecodeFrame(driven.NewCodecs(), header, frame[domain.FrameHeaderSize:])

	if err != nil {
		t.Fatalf("a frame that fell back could not be read without the codec: %v", err)
	}
	if !bytes.Equal(got, entries) {
		t.Error("the entries did not survive the fallback")
	}
}

// Both ways round, the entries come back exactly as they went in.
func TestEncodeFrame_roundTripsEitherWay(t *testing.T) {
	codecs := driven.NewCodecs(rleCodec{})
	for _, payload := range []string{strings.Repeat("a", 300), "abcdefghijklmnopqrstuvwxyz"} {
		entries := entriesFrom(0, payload)
		frame, err := EncodeFrame(rleCodec{}, 0, 1, entries)
		if err != nil {
			t.Fatal(err)
		}
		header := headerOf(t, frame)

		got, err := DecodeFrame(codecs, header, frame[domain.FrameHeaderSize:])

		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, entries) {
			t.Errorf("payload of %d bytes did not survive the round trip", len(payload))
		}
	}
}

// expandingCodec always makes its input larger, which is the worst a codec can do and the
// case the fallback exists for.
type expandingCodec struct{}

func (expandingCodec) ID() driven.CodecID { return driven.CodecID(201) }

func (expandingCodec) Encode(dst, src []byte) ([]byte, error) {
	dst = append(dst, src...)
	return append(dst, bytes.Repeat([]byte{0xff}, 64)...), nil
}

func (expandingCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src[:len(src)-64]...), nil
}

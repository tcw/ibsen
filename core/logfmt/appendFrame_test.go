package logfmt

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// TestAppendFrame_buildsABlockInOneBuffer is what a write larger than one frame does: the
// frames are built into the buffer the store will be handed, one after another, and what was
// already in it is untouched.
func TestAppendFrame_buildsABlockInOneBuffer(t *testing.T) {
	block, err := AppendFrame(nil, driven.NoCodec{}, 0, 2, entriesFrom(0, "first", "second"))
	if err != nil {
		t.Fatal(err)
	}
	firstFrame := len(block)
	block, err = AppendFrame(block, driven.NoCodec{}, 2, 1, entriesFrom(2, "third"))
	if err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(block)
	for _, want := range []struct {
		firstOffset domain.Offset
		entryCount  uint32
	}{{0, 2}, {2, 1}} {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		if header.FirstOffset != want.firstOffset || header.EntryCount != want.entryCount {
			t.Fatalf("frame starts at %d holding %d entries, want %d and %d",
				header.FirstOffset, header.EntryCount, want.firstOffset, want.entryCount)
		}
		if _, err = domain.ReadFramePayload(reader, header); err != nil {
			t.Fatalf("payload of the frame at %d: %v", header.FirstOffset, err)
		}
	}
	if reader.Len() != 0 {
		t.Fatalf("%d bytes left after both frames", reader.Len())
	}
	if headerOf(t, block[firstFrame:]).FirstOffset != 2 {
		t.Fatal("the second frame does not start where the first one ended")
	}
}

// TestAppendFrame_leavesTheBufferAsItWasOnFailure matters because the buffer is the block: a
// frame that could not be built must not leave half of itself in front of the frames that
// were.
func TestAppendFrame_leavesTheBufferAsItWasOnFailure(t *testing.T) {
	block, err := AppendFrame(nil, driven.NoCodec{}, 0, 1, entriesFrom(0, "kept"))
	if err != nil {
		t.Fatal(err)
	}
	whole := append([]byte(nil), block...)

	block, err = AppendFrame(block, failingCodec{}, 1, 1, entriesFrom(1, "lost"))
	if !errors.Is(err, errFailingCodec) {
		t.Fatalf("err=%v, want the codec's own", err)
	}
	if !bytes.Equal(block, whole) {
		t.Fatalf("the buffer holds %d bytes, want the %d it had", len(block), len(whole))
	}
}

// TestDecodeFrame_handsBackThePayloadOfAnUncompressedFrame pins the read path's one
// allocation per frame. A frame written without a codec holds its entries as they are, so
// the bytes read for it are the bytes handed on, and the entries a read hands out point into
// them.
func TestDecodeFrame_handsBackThePayloadOfAnUncompressedFrame(t *testing.T) {
	entries := entriesFrom(0, "first", "second")
	frame, err := AppendFrame(nil, driven.NoCodec{}, 0, 2, entries)
	if err != nil {
		t.Fatal(err)
	}
	header := headerOf(t, frame)
	payload := frame[domain.FrameHeaderSize:]

	decoded, err := DecodeFrame(driven.NewCodecs(), header, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, entries) {
		t.Fatal("the frame does not decode to the entries it was given")
	}
	payload[0] ^= 0xff
	if decoded[0] != payload[0] {
		t.Fatal("the entries were copied out of the payload the read filled")
	}
}

// A compressed frame is another matter: its entries only exist once decoded, so they come
// back in a buffer of the codec's own.
func TestDecodeFrame_decodesACompressedFrame(t *testing.T) {
	entries := entriesFrom(0, strings.Repeat("a", 300))
	frame, err := AppendFrame(nil, rleCodec{}, 0, 1, entries)
	if err != nil {
		t.Fatal(err)
	}
	header := headerOf(t, frame)
	if driven.CodecID(header.Codec) == driven.CodecNone {
		t.Fatal("this payload should have compressed")
	}
	decoded, err := DecodeFrame(driven.NewCodecs(rleCodec{}), header, frame[domain.FrameHeaderSize:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, entries) {
		t.Fatal("the frame does not decode to the entries it was given")
	}
}

var errFailingCodec = errors.New("this codec cannot encode")

// failingCodec is a codec whose encode fails, which is the only way a frame fails to be
// built without a payload of two gigabytes.
type failingCodec struct{}

func (failingCodec) ID() driven.CodecID { return driven.CodecZstd }

func (failingCodec) Encode(dst, src []byte) ([]byte, error) {
	// it writes before it fails, which is what makes the test worth running
	return append(dst, src...), errFailingCodec
}

func (failingCodec) Decode(dst, src []byte, plainSize int) ([]byte, error) {
	return nil, errFailingCodec
}

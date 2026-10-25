package driven

import (
	"errors"
	"testing"
)

// reverse is a stand-in for a real codec: it changes the bytes, so a test can tell whether
// the right codec was picked, and it is its own inverse.
type reverse struct{}

func (reverse) ID() CodecID { return CodecID(42) }

func (reverse) Encode(dst, src []byte) ([]byte, error) {
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst, nil
}

func (r reverse) Decode(dst, src []byte, _ int) ([]byte, error) {
	return r.Encode(dst, src)
}

func TestNoCodecRoundTrip(t *testing.T) {
	stored, err := NoCodec{}.Encode(nil, []byte("entries"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := NoCodec{}.Decode(nil, stored, len(stored))
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != "entries" {
		t.Errorf("round trip gave %q", plain)
	}
}

// A codec is told how many bytes the frame says it should produce, and one that would
// produce a different number says so rather than handing back something else.
func TestNoCodecRefusesAPlainSizeItCannotMeet(t *testing.T) {
	if _, err := (NoCodec{}).Decode(nil, []byte("entries"), 99); err == nil {
		t.Error("decode accepted a payload that does not match the size in the header")
	}
}

// Encode must leave its input alone: the caller reuses the buffer for the next frame.
func TestNoCodecDoesNotTouchItsInput(t *testing.T) {
	src := []byte("entries")
	if _, err := (NoCodec{}).Encode(make([]byte, 0, 7), src); err != nil {
		t.Fatal(err)
	}
	if string(src) != "entries" {
		t.Errorf("Encode changed its input to %q", src)
	}
}

// Whatever else a build wires, a frame written without compression is readable, so the
// identity codec is never missing from a registry.
func TestNewCodecsAlwaysHoldsNoCodec(t *testing.T) {
	codecs := NewCodecs(reverse{})

	codec, err := codecs.Get(CodecNone)
	if err != nil {
		t.Fatal(err)
	}
	if codec.ID() != CodecNone {
		t.Errorf("CodecNone resolved to %s", codec.ID())
	}
	if codec, err = codecs.Get(CodecID(42)); err != nil || codec.ID() != 42 {
		t.Errorf("the wired codec resolved to %v, %v", codec, err)
	}
}

// A frame naming a codec this build did not wire is intact, not damaged. Saying which codec
// is missing is the difference between an operator fixing the deployment and an operator
// looking for data loss.
func TestAnUnwiredCodecIsReportedByName(t *testing.T) {
	_, err := NewCodecs().Get(CodecZstd)

	if !errors.Is(err, ErrUnknownCodec) {
		t.Fatalf("got %v, want ErrUnknownCodec", err)
	}
	if got := err.Error(); got != "unknown codec: codec zstd" {
		t.Errorf("error reads %q, which does not name the codec", got)
	}
}

// A core built with no registry at all still reads what needs no codec, which is what lets
// an embedded build link none of this.
func TestANilRegistryStillReadsUncompressedFrames(t *testing.T) {
	var codecs Codecs

	codec, err := codecs.Get(CodecNone)
	if err != nil {
		t.Fatal(err)
	}
	if codec.ID() != CodecNone {
		t.Errorf("CodecNone resolved to %s", codec.ID())
	}
	if _, err = codecs.Get(CodecZstd); !errors.Is(err, ErrUnknownCodec) {
		t.Errorf("a nil registry gave %v for zstd, want ErrUnknownCodec", err)
	}
}

func TestCodecIDString(t *testing.T) {
	for id, want := range map[CodecID]string{
		CodecNone: "none",
		CodecZstd: "zstd",
		200:       "CodecID(200)",
	} {
		if got := id.String(); got != want {
			t.Errorf("CodecID(%d) reads %q, want %q", uint8(id), got, want)
		}
	}
}

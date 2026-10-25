package domain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func sampleHeader() FrameHeader {
	return FrameHeader{
		Codec:       1,
		FirstOffset: 4096,
		EntryCount:  7,
		StoredSize:  31,
		PlainSize:   64,
		PayloadCrc:  0xdeadbeef,
	}
}

// reseal recomputes the header checksum over a header someone has edited, which is what a
// test needs to ask "is this field acted on once the checksum says it may be".
func reseal(header []byte) {
	binary.LittleEndian.PutUint32(header[4:], crc32.Checksum(header[8:FrameHeaderSize], crc32q))
}

func TestFrameHeaderRoundTrip(t *testing.T) {
	want := sampleHeader()
	encoded := AppendFrameHeader(nil, want)
	if len(encoded) != FrameHeaderSize {
		t.Fatalf("header is %d bytes, want %d", len(encoded), FrameHeaderSize)
	}

	got, err := ReadFrameHeader(bytes.NewReader(encoded), MaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("read back %+v, want %+v", got, want)
	}
	if got.Size() != FrameHeaderSize+int64(want.StoredSize) {
		t.Errorf("frame size is %d, want header plus payload", got.Size())
	}
	if got.EndOffset() != 4103 {
		t.Errorf("end offset is %d, want 4103", got.EndOffset())
	}
	for _, offset := range []Offset{4096, 4099, 4102} {
		if !got.Contains(offset) {
			t.Errorf("frame does not claim offset %d, which is in 4096..4102", offset)
		}
	}
	for _, offset := range []Offset{4095, 4103} {
		if got.Contains(offset) {
			t.Errorf("frame claims offset %d, which is outside 4096..4102", offset)
		}
	}
}

// Every byte of a header is covered by its checksum, so there is no field a flipped bit can
// change quietly. That matters more here than for an entry: the header is what says how many
// bytes to read next and what they should hash to.
func TestEveryHeaderByteIsCoveredByItsChecksum(t *testing.T) {
	encoded := AppendFrameHeader(nil, sampleHeader())
	for i := 0; i < FrameHeaderSize; i++ {
		damaged := append([]byte(nil), encoded...)
		damaged[i] ^= 0xff

		_, err := ReadFrameHeader(bytes.NewReader(damaged), MaxFrameSize)

		if !errors.Is(err, ErrCorruptFrame) {
			t.Errorf("byte %d changed without being caught: err=%v", i, err)
		}
	}
}

// A block that never held a frame is told apart from one that holds a damaged frame, since
// the two call for opposite treatment: cut the damage, keep what you do not understand.
func TestAHeaderWithoutTheMagicIsNotAFrame(t *testing.T) {
	encoded := AppendFrameHeader(nil, sampleHeader())
	binary.LittleEndian.PutUint32(encoded[0:], FrameMagic+1)

	_, err := ReadFrameHeader(bytes.NewReader(encoded), MaxFrameSize)

	if !errors.Is(err, ErrNotAFrame) {
		t.Fatalf("got %v, want ErrNotAFrame", err)
	}
	if !errors.Is(err, ErrCorruptFrame) {
		t.Error("ErrNotAFrame must also be a corrupt frame, so a scan mid-block truncates from it")
	}
}

// A frame written by a newer Ibsen is refused rather than guessed at.
func TestAFutureFrameVersionIsUnsupported(t *testing.T) {
	encoded := AppendFrameHeader(nil, sampleHeader())
	encoded[9] = FrameVersion + 1
	reseal(encoded)

	_, err := ReadFrameHeader(bytes.NewReader(encoded), MaxFrameSize)

	if !errors.Is(err, ErrUnsupportedLogFormat) {
		t.Fatalf("got %v, want ErrUnsupportedLogFormat", err)
	}
}

// No length is acted on before the checksum has vouched for it, and one that cannot fit in
// what is left of the block is corruption rather than something to allocate.
func TestAStoredSizeLargerThanWhatIsLeftIsCorrupt(t *testing.T) {
	header := sampleHeader()
	header.StoredSize = 4096
	encoded := AppendFrameHeader(nil, header)

	_, err := ReadFrameHeader(bytes.NewReader(encoded), 100)

	if !errors.Is(err, ErrCorruptFrame) {
		t.Fatalf("got %v, want ErrCorruptFrame", err)
	}
}

func TestReadFrameHeader_boundaries(t *testing.T) {
	encoded := AppendFrameHeader(nil, sampleHeader())

	if _, err := ReadFrameHeader(bytes.NewReader(nil), MaxFrameSize); err != io.EOF {
		t.Errorf("an empty reader gave %v, want io.EOF: that is a clean frame boundary", err)
	}
	if _, err := ReadFrameHeader(bytes.NewReader(encoded[:20]), MaxFrameSize); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a half-written header gave %v, want io.ErrUnexpectedEOF", err)
	}
}

// framed is a header and a payload, the way a block holds them.
func framed(t *testing.T, payload []byte) ([]byte, FrameHeader) {
	t.Helper()
	header := FrameHeader{
		FirstOffset: 0,
		EntryCount:  1,
		StoredSize:  uint32(len(payload)),
		PlainSize:   uint32(len(payload)),
		PayloadCrc:  crc32.Checksum(payload, crc32q),
	}
	return append(AppendFrameHeader(nil, header), payload...), header
}

func TestFramePayload_readVerifySkipAgree(t *testing.T) {
	// larger than the buffer VerifyFramePayload streams through, so it covers more than one
	// pass of the loop as well as the short case
	for _, size := range []int{0, 11, verifyChunk + 7} {
		payload := bytes.Repeat([]byte("payload!"), size/8+1)[:size]
		frame, header := framed(t, payload)
		body := frame[FrameHeaderSize:]

		got, err := ReadFramePayload(bytes.NewReader(body), header)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("size %d: payload did not survive the round trip", size)
		}
		if err = VerifyFramePayload(bytes.NewReader(body), header); err != nil {
			t.Errorf("size %d: verify rejected a payload read accepts: %v", size, err)
		}
		rest := bytes.NewReader(append(append([]byte(nil), body...), 'x'))
		if err = SkipFramePayload(rest, header); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if left, _ := io.ReadAll(rest); string(left) != "x" {
			t.Errorf("size %d: skip left %q, want the byte after the payload", size, left)
		}
	}
}

func TestFramePayload_damageIsCaughtBothWays(t *testing.T) {
	payload := bytes.Repeat([]byte("payload!"), verifyChunk/8)
	frame, header := framed(t, payload)
	damaged := append([]byte(nil), frame[FrameHeaderSize:]...)
	damaged[len(damaged)/2] ^= 0xff

	if _, err := ReadFramePayload(bytes.NewReader(damaged), header); !errors.Is(err, ErrCorruptFrame) {
		t.Errorf("read accepted a damaged payload: %v", err)
	}
	if err := VerifyFramePayload(bytes.NewReader(damaged), header); !errors.Is(err, ErrCorruptFrame) {
		t.Errorf("verify accepted a damaged payload: %v", err)
	}
}

// A payload cut short is a torn write, not corruption, and both readers say so the same way.
func TestFramePayload_aShortPayloadIsTorn(t *testing.T) {
	frame, header := framed(t, bytes.Repeat([]byte("payload!"), 8))
	short := frame[FrameHeaderSize : len(frame)-10]

	if _, err := ReadFramePayload(bytes.NewReader(short), header); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("read gave %v, want io.ErrUnexpectedEOF", err)
	}
	if err := VerifyFramePayload(bytes.NewReader(short), header); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("verify gave %v, want io.ErrUnexpectedEOF", err)
	}
	if err := SkipFramePayload(bytes.NewReader(short), header); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("skip gave %v, want io.ErrUnexpectedEOF", err)
	}
}

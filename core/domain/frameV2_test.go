package domain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
)

func v2Header() FrameHeader {
	return FrameHeader{Codec: 1, FirstOffset: 42, EntryCount: 3, StoredSize: 10, PlainSize: 20, PayloadCrc: 7,
		Producer: ProducerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Sequence: 99, WriteEntries: 9, DictID: 0xABCDEF}
}

func TestFrameV2_roundTrip(t *testing.T) {
	want := v2Header()
	encoded := AppendFrameHeader(nil, want)
	if len(encoded) != FrameHeaderSize+FrameExtensionSize || int64(len(encoded)) != want.HeaderSize() {
		// the full extension: flags, producer, dictionary, checksum
		t.Fatalf("a v2 header is %d bytes", len(encoded))
	}
	got, err := ReadFrameHeader(bytes.NewReader(encoded), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got.Size() != FrameHeaderSize+FrameExtensionSize+10 {
		t.Errorf("Size is %d", got.Size())
	}
}

// A header with neither a producer nor a dictionary is version 1, byte for byte as before.
func TestFrameV2_withoutAnExtensionIsVersion1(t *testing.T) {
	plain := FrameHeader{FirstOffset: 1, EntryCount: 1, StoredSize: 5, PlainSize: 5}
	encoded := AppendFrameHeader(nil, plain)
	if len(encoded) != FrameHeaderSize || encoded[9] != FrameVersion {
		t.Fatalf("a header with no extension is %d bytes, version %d", len(encoded), encoded[9])
	}
	withProducerOnly := plain
	withProducerOnly.Producer = ProducerID{1}
	if AppendFrameHeader(nil, withProducerOnly)[9] != FrameVersionExtended {
		t.Error("a producer did not make version 2")
	}
	withDictOnly := plain
	withDictOnly.DictID = 1
	if AppendFrameHeader(nil, withDictOnly)[9] != FrameVersionExtended {
		t.Error("a dictionary did not make version 2")
	}
}

func TestFrameV2_everyExtensionByteIsChecked(t *testing.T) {
	encoded := AppendFrameHeader(nil, v2Header())
	for i := FrameHeaderSize; i < len(encoded); i++ {
		damaged := append([]byte(nil), encoded...)
		damaged[i] ^= 0x40
		if _, err := ReadFrameHeader(bytes.NewReader(damaged), 1000); !errors.Is(err, ErrCorruptFrame) {
			t.Errorf("byte %d of the extension flipped: got %v, want ErrCorruptFrame", i, err)
		}
	}
}

func TestFrameV2_aCutExtensionIsTorn(t *testing.T) {
	encoded := AppendFrameHeader(nil, v2Header())
	for cut := FrameHeaderSize; cut < len(encoded); cut++ {
		if _, err := ReadFrameHeader(bytes.NewReader(encoded[:cut]), 1000); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("cut at %d: got %v, want io.ErrUnexpectedEOF", cut, err)
		}
	}
}

func TestFrameV2_anUnknownExtensionLengthIsUnsupported(t *testing.T) {
	encoded := AppendFrameHeader(nil, v2Header())
	// an extension longer than its flags take, with both checksums right: a newer build's
	// frame, carrying a field this one does not know
	ext := append(append([]byte(nil), encoded[FrameHeaderSize:len(encoded)-4]...), make([]byte, 8)...)
	ext = binary.LittleEndian.AppendUint32(ext, crc32.Checksum(ext, crc32q))
	encoded = append(encoded[:FrameHeaderSize], ext...)
	binary.LittleEndian.PutUint16(encoded[10:], uint16(len(ext)))
	binary.LittleEndian.PutUint32(encoded[4:], crc32.Checksum(encoded[8:36], crc32q))
	if _, err := ReadFrameHeader(bytes.NewReader(encoded), 1000); !errors.Is(err, ErrUnsupportedLogFormat) {
		t.Fatalf("got %v, want ErrUnsupportedLogFormat", err)
	}
}

// readHeaderAsVersion1Did is ReadFrameHeader as it was before version 2: verify the 28 bytes
// after the checksum, then refuse any version but 1. It is what an older Ibsen does with a
// block a newer one wrote, and it must say "unsupported", which recovery never truncates,
// rather than "corrupt", which it does.
func readHeaderAsVersion1Did(r io.Reader) error {
	var encoded [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, encoded[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(encoded[0:]) != FrameMagic {
		return ErrNotAFrame
	}
	body := encoded[8:]
	if binary.LittleEndian.Uint32(encoded[4:]) != crc32.Checksum(body, crc32q) {
		return fmt.Errorf("%w: header checksum mismatch", ErrCorruptFrame)
	}
	if body[1] != 1 {
		return fmt.Errorf("%w: frame version %d", ErrUnsupportedLogFormat, body[1])
	}
	return nil
}

func TestFrameV2_anOlderBuildRefusesItRatherThanTruncating(t *testing.T) {
	err := readHeaderAsVersion1Did(bytes.NewReader(AppendFrameHeader(nil, v2Header())))
	if !errors.Is(err, ErrUnsupportedLogFormat) {
		t.Fatalf("an older build reads a v2 frame as %v, want ErrUnsupportedLogFormat", err)
	}
	if err := readHeaderAsVersion1Did(bytes.NewReader(AppendFrameHeader(nil, FrameHeader{EntryCount: 1}))); err != nil {
		t.Fatalf("an older build refuses a v1 frame written now: %v", err)
	}
}

func TestProducerID_parsesAndPrints(t *testing.T) {
	id, err := ParseProducerID("01020304-0506-0708-090a-0b0c0d0e0f10")
	if err != nil || id != (ProducerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) {
		t.Fatalf("got %v, %v", id, err)
	}
	if id.String() != "01020304-0506-0708-090a-0b0c0d0e0f10" {
		t.Errorf("printed %s", id)
	}
	if again, _ := ParseProducerID("0102030405060708090a0b0c0d0e0f10"); again != id {
		t.Error("the form without hyphens differs")
	}
	if none, err := ParseProducerID(""); err != nil || !none.IsZero() {
		t.Error("an empty id is not no producer")
	}
	for _, bad := range []string{"xyz", "0102", "01020304-0506-0708-090a-0b0c0d0e0f1g"} {
		if _, err := ParseProducerID(bad); !errors.Is(err, ErrInvalidProducerID) {
			t.Errorf("%q: got %v", bad, err)
		}
	}
}

// A frame carries only the fields it uses: a dictionary alone costs 13 bytes, a producer
// alone 33, both 41.
func TestFrameV2_carriesOnlyWhatItUses(t *testing.T) {
	for _, tc := range []struct {
		h    FrameHeader
		size int64
	}{
		{FrameHeader{DictID: 9}, FrameHeaderSize + 13},
		{FrameHeader{Producer: ProducerID{1}, Sequence: 2, WriteEntries: 3}, FrameHeaderSize + 33},
		{FrameHeader{Producer: ProducerID{1}, Sequence: 2, WriteEntries: 3, DictID: 9}, FrameHeaderSize + 41},
	} {
		encoded := AppendFrameHeader(nil, tc.h)
		if int64(len(encoded)) != tc.size {
			t.Errorf("%+v is %d bytes, want %d", tc.h, len(encoded), tc.size)
		}
		got, err := ReadFrameHeader(bytes.NewReader(encoded), 1000)
		if err != nil || got != tc.h {
			t.Errorf("read back %+v, %v; want %+v", got, err, tc.h)
		}
	}
}

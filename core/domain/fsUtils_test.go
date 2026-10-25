package domain

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestEntry_roundTrip(t *testing.T) {
	encoded := AppendEntry(nil, []byte("payload"), 42)
	entry, n, err := ParseEntry(encoded, MaxEntrySize)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(encoded) || entry.Offset != 42 || entry.ByteSize != 7 || string(entry.Entry) != "payload" {
		t.Fatalf("got %+v, n=%d", entry, n)
	}
	if entry.Crc != binary.LittleEndian.Uint32(encoded) {
		t.Fatalf("crc %d, want %d", entry.Crc, binary.LittleEndian.Uint32(encoded))
	}
}

// TestAppendEntry_appendsToWhatIsThere is the property the frame payload rests on: entries
// are encoded one after another into the buffer they will be stored in, and each one reads
// back where the one before it ended.
func TestAppendEntry_appendsToWhatIsThere(t *testing.T) {
	payloads := []string{"first", "", "third and longer"}
	var buf []byte
	for i, payload := range payloads {
		buf = AppendEntry(buf, []byte(payload), Offset(i))
	}
	at := 0
	for i, payload := range payloads {
		entry, n, err := ParseEntry(buf[at:], MaxEntrySize)
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
		if string(entry.Entry) != payload || entry.Offset != uint64(i) {
			t.Fatalf("entry %d is %+v, want %q at offset %d", i, entry, payload, i)
		}
		at += n
	}
	if at != len(buf) {
		t.Fatalf("the entries cover %d of %d bytes", at, len(buf))
	}
	if _, _, err := ParseEntry(buf[at:], MaxEntrySize); err != io.EOF {
		t.Fatalf("past the last entry: err=%v, want io.EOF", err)
	}
}

// TestEntry_allocatesNothingPerEntry is why these two functions have the shape they do. A
// write of a million entries should allocate the payload buffer it is building and nothing
// else, and a read of them should allocate the frame it decoded and nothing else; the entry
// aliases the buffer it was parsed from.
func TestEntry_allocatesNothingPerEntry(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates on its own account, so the count here is not this package's")
	}
	payload := []byte("an entry of some ordinary size, as a log holds")
	buf := make([]byte, 0, 4096)
	encoding := testing.AllocsPerRun(1000, func() {
		buf = AppendEntry(buf[:0], payload, 7)
	})
	if encoding != 0 {
		t.Fatalf("encoding an entry into a buffer with room allocated %v times, want 0", encoding)
	}
	parsing := testing.AllocsPerRun(1000, func() {
		if _, _, err := ParseEntry(buf, MaxEntrySize); err != nil {
			t.Fatal(err)
		}
	})
	if parsing != 0 {
		t.Fatalf("parsing an entry allocated %v times, want 0", parsing)
	}
}

// TestParseEntry_aliasesItsSource pins the ownership rule the read path depends on: the
// entry is a window onto the buffer it was parsed from, not a copy of it.
func TestParseEntry_aliasesItsSource(t *testing.T) {
	buf := AppendEntry(nil, []byte("payload"), 0)
	entry, _, err := ParseEntry(buf, MaxEntrySize)
	if err != nil {
		t.Fatal(err)
	}
	buf[12] = 'P'
	if string(entry.Entry) != "Payload" {
		t.Fatalf("the entry holds %q, so it copied its source", entry.Entry)
	}
}

func TestParseEntry_failures(t *testing.T) {
	encoded := AppendEntry(nil, []byte("payload"), 42)
	flipped := func(i int) []byte {
		c := append([]byte(nil), encoded...)
		c[i] ^= 0xff
		return c
	}
	tests := []struct {
		name    string
		input   []byte
		maxSize uint64
		want    error
	}{
		{name: "empty", input: nil, maxSize: MaxEntrySize, want: io.EOF},
		{name: "partial header", input: encoded[:5], maxSize: MaxEntrySize, want: io.ErrUnexpectedEOF},
		{name: "partial body", input: encoded[:len(encoded)-1], maxSize: MaxEntrySize, want: io.ErrUnexpectedEOF},
		{name: "flipped checksum", input: flipped(0), maxSize: MaxEntrySize, want: ErrCorruptEntry},
		{name: "flipped payload", input: flipped(12), maxSize: MaxEntrySize, want: ErrCorruptEntry},
		{name: "flipped offset", input: flipped(len(encoded) - 1), maxSize: MaxEntrySize, want: ErrCorruptEntry},
		{name: "size field beyond max", input: flipped(11), maxSize: MaxEntrySize, want: ErrCorruptEntry},
		{name: "size over caller limit", input: encoded, maxSize: 6, want: ErrCorruptEntry},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := ParseEntry(test.input, test.maxSize)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v, want %v", err, test.want)
			}
		})
	}
}

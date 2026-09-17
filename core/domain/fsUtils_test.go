package domain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestReadEntry_roundTrip(t *testing.T) {
	encoded := CreateByteEntry([]byte("payload"), 42)
	entry, n, err := ReadEntry(bytes.NewReader(encoded), MaxEntrySize)
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

func TestReadEntry_failures(t *testing.T) {
	encoded := CreateByteEntry([]byte("payload"), 42)
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
			_, _, err := ReadEntry(bytes.NewReader(test.input), test.maxSize)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v, want %v", err, test.want)
			}
		})
	}
}

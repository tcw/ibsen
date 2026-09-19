package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// EntryOverhead is the bytes a log entry adds around its payload: crc (4), size (8) and offset (8).
const EntryOverhead = 20

// entryHeaderSize is the crc and the size, which is what has to be there before the payload
// size can be trusted.
const entryHeaderSize = 12

// MaxEntrySize bounds a payload size read from disk, so a corrupt size field is reported instead of allocated.
const MaxEntrySize = math.MaxInt32 - EntryOverhead

var crc32q = crc32.MakeTable(crc32.Castagnoli)

var ErrCorruptEntry = errors.New("corrupt log entry")

// AppendEntry encodes one log entry into dst and returns the extended slice, the way append
// does. Nothing is allocated for the entry: dst is the frame payload being built, so an
// entry is written where it is going to be stored rather than assembled beside it and copied
// in. A write of a million small entries is a million entries the garbage collector never
// hears about.
//
// The layout is crc32c(4) | size uint64 LE (8) | entry | offset uint64 LE (8). The checksum
// covers everything after it, and is taken from the bytes as written rather than from the
// values they came from, so what is checked is what is stored.
func AppendEntry(dst []byte, entry []byte, currentOffset Offset) []byte {
	start := len(dst)
	// room for the checksum, which is taken once the bytes it covers are written
	dst = append(dst, 0, 0, 0, 0)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(len(entry)))
	dst = append(dst, entry...)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(currentOffset))
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], crc32q))
	return dst
}

// ParseEntry decodes one log entry from the start of src and verifies its checksum. It
// returns io.EOF only when src is empty (a clean entry boundary), io.ErrUnexpectedEOF for an
// entry cut short, and ErrCorruptEntry for a checksum mismatch or a payload larger than
// maxSize. n is the number of bytes the entry occupies.
//
// The entry aliases src instead of copying out of it. A read decodes a whole frame into a
// buffer of its own and hands out the entries inside it, so the copy would be of every byte
// read and would buy nothing; src must not be changed while the entry is in use.
func ParseEntry(src []byte, maxSize uint64) (entry LogEntry, n int, err error) {
	if maxSize > MaxEntrySize {
		maxSize = MaxEntrySize
	}
	if len(src) == 0 {
		return LogEntry{}, 0, io.EOF
	}
	if len(src) < entryHeaderSize {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	size := binary.LittleEndian.Uint64(src[4:entryHeaderSize])
	if size > maxSize {
		return LogEntry{}, 0, fmt.Errorf("%w: payload size %d exceeds %d", ErrCorruptEntry, size, maxSize)
	}
	end := uint64(EntryOverhead) + size
	if uint64(len(src)) < end {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	crc := binary.LittleEndian.Uint32(src)
	if crc32.Checksum(src[4:end], crc32q) != crc {
		return LogEntry{}, 0, fmt.Errorf("%w: checksum mismatch", ErrCorruptEntry)
	}
	payloadEnd := uint64(entryHeaderSize) + size
	return LogEntry{
		Offset:   binary.LittleEndian.Uint64(src[payloadEnd:end]),
		Crc:      crc,
		ByteSize: int(size),
		Entry:    src[entryHeaderSize:payloadEnd:payloadEnd],
	}, int(end), nil
}

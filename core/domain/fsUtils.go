package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/tcw/ibsen/utils"
)

// EntryOverhead is the bytes a log entry adds around its payload: crc (4), size (8) and offset (8).
const EntryOverhead = 20

// MaxEntrySize bounds a payload size read from disk, so a corrupt size field is reported instead of allocated.
const MaxEntrySize = math.MaxInt32 - EntryOverhead

var crc32q = crc32.MakeTable(crc32.Castagnoli)

var ErrCorruptEntry = errors.New("corrupt log entry")

func CreateByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := Uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := Uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(byteSize, crc32q)
	checksum = crc32.Update(checksum, crc32q, entry)
	checksum = crc32.Update(checksum, crc32q, offset)
	check := Uint32ToLittleEndian(checksum)
	return utils.JoinSize(EntryOverhead+entrySize, check, byteSize, entry, offset)
}

// ReadEntry reads one log entry from r and verifies its checksum. It returns io.EOF only
// when r ends exactly on an entry boundary, io.ErrUnexpectedEOF for a partial entry, and
// ErrCorruptEntry for a checksum mismatch or a payload larger than maxSize. n is the
// number of bytes the entry occupies.
func ReadEntry(r io.Reader, maxSize uint64) (entry LogEntry, n int, err error) {
	if maxSize > MaxEntrySize {
		maxSize = MaxEntrySize
	}
	header := make([]byte, 12)
	if _, err = io.ReadFull(r, header); err != nil {
		return LogEntry{}, 0, err
	}
	size := binary.LittleEndian.Uint64(header[4:])
	if size > maxSize {
		return LogEntry{}, 0, fmt.Errorf("%w: payload size %d exceeds %d", ErrCorruptEntry, size, maxSize)
	}
	body := make([]byte, size+8)
	if _, err = io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return LogEntry{}, 0, err
	}
	crc := binary.LittleEndian.Uint32(header)
	if crc32.Update(crc32.Checksum(header[4:], crc32q), crc32q, body) != crc {
		return LogEntry{}, 0, fmt.Errorf("%w: checksum mismatch", ErrCorruptEntry)
	}
	return LogEntry{
		Offset:   binary.LittleEndian.Uint64(body[size:]),
		Crc:      crc,
		ByteSize: int(size),
		Entry:    body[:size:size],
	}, int(size) + EntryOverhead, nil
}

func Uint64ArrayToBytes(uintArray []uint64) []byte {
	var bytes []byte
	for _, value := range uintArray {
		bytes = append(bytes, Uint64ToLittleEndian(value)...)
	}
	return bytes
}

func Uint64ToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func Uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

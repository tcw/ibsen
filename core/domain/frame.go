package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// A log block is a sequence of frames, and a frame holds one write batch: the entries as
// CreateByteEntry encodes them, put through a codec, behind a header that says enough to
// place the frame in the log, skip it, and check it, without decoding it.
//
// That last property is what the header is worth its bytes for. Recovery walks a block
// frame by frame on checksums alone, so a binary that does not carry the codec a frame was
// written with can still find a torn tail and cut it. Navigation is the same: a read
// looking for an offset skips whole frames by their stored size and decodes only the one
// frame that holds it.
//
// The layout, little endian throughout:
//
//	magic       uint32  FrameMagic, which tells a block written before framing from a corrupt one
//	headerCrc   uint32  crc32c over the 28 bytes after it
//	codec       uint8   the codec the payload went through
//	version     uint8   FrameVersion
//	reserved    uint16  zero
//	firstOffset uint64  the offset of the frame's first entry
//	entryCount  uint32  how many entries the frame holds
//	storedSize  uint32  payload bytes as stored, following the header
//	plainSize   uint32  payload bytes once decoded
//	payloadCrc  uint32  crc32c over the stored payload
//
// The header checksum covers the payload checksum, so a header that verifies can be trusted
// about how big its payload is and what it must hash to. No length read from a block is
// acted on before it has been checked, which is the same rule ReadEntry follows.
const (
	// FrameHeaderSize is the bytes a frame adds in front of its payload.
	FrameHeaderSize = 36

	// FrameMagic is "IBSF" read as a little endian uint32. A log block starts with it, so a
	// block written before framing is reported as such instead of as corruption.
	FrameMagic uint32 = 0x46534249

	// FrameVersion is the frame layout this build writes. A frame numbered higher was
	// written by a newer Ibsen and is not guessed at.
	FrameVersion uint8 = 1

	// MaxFrameSize bounds a payload size read from a block, so a corrupt length is reported
	// instead of allocated.
	MaxFrameSize = math.MaxInt32 - FrameHeaderSize
)

var (
	// ErrCorruptFrame is a frame that does not verify: a bad checksum, or a length that
	// cannot be right.
	ErrCorruptFrame = errors.New("corrupt log frame")

	// ErrNotAFrame is a header that does not start with FrameMagic. It is a corrupt frame,
	// so a scan in the middle of a block truncates from it like any other; only at the very
	// start of a block does it mean something more specific, which RecoverBlock reports.
	ErrNotAFrame = fmt.Errorf("%w: not a frame header", ErrCorruptFrame)

	// ErrUnsupportedLogFormat is a block this build cannot read at all: one written before
	// framing, or by a newer Ibsen. It is never truncated away, since nothing says the bytes
	// are damaged.
	ErrUnsupportedLogFormat = errors.New("unsupported log format")
)

// FrameHeader is what a frame says about itself.
type FrameHeader struct {
	Codec       uint8
	FirstOffset Offset
	EntryCount  uint32
	StoredSize  uint32
	PlainSize   uint32
	PayloadCrc  uint32
}

// Size is the bytes the whole frame occupies, header and payload.
func (h FrameHeader) Size() int64 {
	return FrameHeaderSize + int64(h.StoredSize)
}

// EndOffset is the offset following the frame's last entry.
func (h FrameHeader) EndOffset() Offset {
	return h.FirstOffset + Offset(h.EntryCount)
}

// Contains reports whether offset is one of the entries the frame holds.
func (h FrameHeader) Contains(offset Offset) bool {
	return offset >= h.FirstOffset && offset < h.EndOffset()
}

// AppendFrameHeader encodes h onto dst and returns the result, the way append does.
func AppendFrameHeader(dst []byte, h FrameHeader) []byte {
	var encoded [FrameHeaderSize]byte
	binary.LittleEndian.PutUint32(encoded[0:], FrameMagic)
	body := encoded[8:]
	body[0] = h.Codec
	body[1] = FrameVersion
	binary.LittleEndian.PutUint16(body[2:], 0)
	binary.LittleEndian.PutUint64(body[4:], uint64(h.FirstOffset))
	binary.LittleEndian.PutUint32(body[12:], h.EntryCount)
	binary.LittleEndian.PutUint32(body[16:], h.StoredSize)
	binary.LittleEndian.PutUint32(body[20:], h.PlainSize)
	binary.LittleEndian.PutUint32(body[24:], h.PayloadCrc)
	binary.LittleEndian.PutUint32(encoded[4:], crc32.Checksum(body, crc32q))
	return append(dst, encoded[:]...)
}

// ReadFrameHeader reads one frame header from r and verifies it. It returns io.EOF only
// when r ends exactly on a frame boundary, io.ErrUnexpectedEOF for a partial header,
// ErrNotAFrame when the magic is missing, ErrUnsupportedLogFormat for a version this build
// does not know, and ErrCorruptFrame for a header that fails its checksum or claims a
// payload larger than maxStored.
func ReadFrameHeader(r io.Reader, maxStored uint64) (FrameHeader, error) {
	if maxStored > MaxFrameSize {
		maxStored = MaxFrameSize
	}
	var encoded [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, encoded[:]); err != nil {
		return FrameHeader{}, err
	}
	if binary.LittleEndian.Uint32(encoded[0:]) != FrameMagic {
		return FrameHeader{}, ErrNotAFrame
	}
	body := encoded[8:]
	if binary.LittleEndian.Uint32(encoded[4:]) != crc32.Checksum(body, crc32q) {
		return FrameHeader{}, fmt.Errorf("%w: header checksum mismatch", ErrCorruptFrame)
	}
	// only now, with the header checked, are its numbers worth reading
	if version := body[1]; version != FrameVersion {
		return FrameHeader{}, fmt.Errorf("%w: frame version %d, this build writes %d",
			ErrUnsupportedLogFormat, version, FrameVersion)
	}
	header := FrameHeader{
		Codec:       body[0],
		FirstOffset: Offset(binary.LittleEndian.Uint64(body[4:])),
		EntryCount:  binary.LittleEndian.Uint32(body[12:]),
		StoredSize:  binary.LittleEndian.Uint32(body[16:]),
		PlainSize:   binary.LittleEndian.Uint32(body[20:]),
		PayloadCrc:  binary.LittleEndian.Uint32(body[24:]),
	}
	if uint64(header.StoredSize) > maxStored {
		return FrameHeader{}, fmt.Errorf("%w: stored size %d exceeds %d", ErrCorruptFrame, header.StoredSize, maxStored)
	}
	if header.PlainSize > MaxFrameSize {
		return FrameHeader{}, fmt.Errorf("%w: plain size %d exceeds %d", ErrCorruptFrame, header.PlainSize, MaxFrameSize)
	}
	return header, nil
}

// ReadFramePayload reads the payload h describes and verifies its checksum. It returns the
// bytes as stored; decoding them is the codec's business. A short read is
// io.ErrUnexpectedEOF, which is a torn write rather than corruption.
func ReadFramePayload(r io.Reader, h FrameHeader) ([]byte, error) {
	payload := make([]byte, h.StoredSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if crc32.Checksum(payload, crc32q) != h.PayloadCrc {
		return nil, fmt.Errorf("%w: payload checksum mismatch", ErrCorruptFrame)
	}
	return payload, nil
}

// SkipFramePayload advances r past the payload h describes without checking it, which is
// what navigating to another frame needs.
func SkipFramePayload(r io.Reader, h FrameHeader) error {
	if _, err := io.CopyN(io.Discard, r, int64(h.StoredSize)); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

// verifyChunk is the buffer VerifyFramePayload streams through; a frame is checked without
// being held, so recovery of a large frame costs no more memory than a small one.
const verifyChunk = 32 * 1024

// VerifyFramePayload reads past the payload h describes, checking it against the checksum in
// the header without keeping it. Recovery walks a whole block this way and decodes nothing,
// which is what lets a build that does not carry the codec a block was written with still
// find its torn tail and cut it.
func VerifyFramePayload(r io.Reader, h FrameHeader) error {
	size := int(h.StoredSize)
	if size > verifyChunk {
		size = verifyChunk
	}
	buf := make([]byte, size)
	var crc uint32
	remaining := int64(h.StoredSize)
	for remaining > 0 {
		chunk := buf
		if int64(len(chunk)) > remaining {
			chunk = buf[:remaining]
		}
		n, err := io.ReadFull(r, chunk)
		crc = crc32.Update(crc, crc32q, chunk[:n])
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		remaining = remaining - int64(n)
	}
	if crc != h.PayloadCrc {
		return fmt.Errorf("%w: payload checksum mismatch", ErrCorruptFrame)
	}
	return nil
}

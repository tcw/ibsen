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
// AppendEntry encodes them, put through a codec, behind a header that says enough to
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
// acted on before it has been checked, which is the same rule ParseEntry follows.
//
// Version 2 adds an extension after those 36 bytes, for a frame that carries a producer (safe
// retries) or names a compression dictionary. The 36 bytes are laid out and checksummed
// exactly as in version 1, with reserved holding the extension's length, so a build that only
// knows version 1 verifies the header, reads version 2, and refuses the block as one it cannot
// read rather than truncating it as damage. The extension, little endian:
//
//	producer     [16]byte  the writer's producer id; zero for none
//	sequence     uint64    the producer's number for the write this frame belongs to
//	writeEntries uint32    how many entries the whole write holds, across all its frames
//	reserved     uint32    zero
//	dictID       uint64    the compression dictionary the payload was encoded with; zero for none
//	extCrc       uint32    crc32c over the 40 bytes before it
//
// A frame with neither a producer nor a dictionary is written as version 1, so a log that
// uses neither looks exactly as it did.
const (
	// FrameHeaderSize is the bytes every frame adds in front of its payload: all of a version 1
	// header, and the part of a version 2 header that precedes its extension.
	FrameHeaderSize = 36

	// FrameExtensionSize is the bytes a version 2 header adds after FrameHeaderSize.
	FrameExtensionSize = 44

	// FrameMagic is "IBSF" read as a little endian uint32. A log block starts with it, so a
	// block written before framing is reported as such instead of as corruption.
	FrameMagic uint32 = 0x46534249

	// FrameVersion is the frame layout without an extension. A frame numbered higher than
	// FrameVersionExtended was written by a newer Ibsen and is not guessed at.
	FrameVersion uint8 = 1

	// FrameVersionExtended is the frame layout with the producer and dictionary extension.
	FrameVersionExtended uint8 = 2

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
	// Producer, Sequence and WriteEntries say which producer's write the frame belongs to,
	// and how many entries that whole write holds; zero Producer for a write without one.
	Producer     ProducerID
	Sequence     uint64
	WriteEntries uint32
	// DictID names the compression dictionary the payload was encoded with; zero for none.
	DictID uint64
}

// Extended reports whether the header needs the version 2 extension.
func (h FrameHeader) Extended() bool {
	return !h.Producer.IsZero() || h.DictID != 0
}

// HeaderSize is the bytes the header occupies.
func (h FrameHeader) HeaderSize() int64 {
	if h.Extended() {
		return FrameHeaderSize + FrameExtensionSize
	}
	return FrameHeaderSize
}

// Size is the bytes the whole frame occupies, header and payload.
func (h FrameHeader) Size() int64 {
	return h.HeaderSize() + int64(h.StoredSize)
}

// EndOffset is the offset following the frame's last entry.
func (h FrameHeader) EndOffset() Offset {
	return h.FirstOffset + Offset(h.EntryCount)
}

// Contains reports whether offset is one of the entries the frame holds.
func (h FrameHeader) Contains(offset Offset) bool {
	return offset >= h.FirstOffset && offset < h.EndOffset()
}

// PutFrameHeader encodes h into the first h.HeaderSize() bytes of dst, which must be at
// least that long. It is how a frame written straight into the buffer it will be stored from
// gets its header: the room is reserved, the payload goes in after it, and the header is
// written once its size and checksum are known.
func PutFrameHeader(dst []byte, h FrameHeader) {
	encoded := dst[:FrameHeaderSize]
	binary.LittleEndian.PutUint32(encoded[0:], FrameMagic)
	body := encoded[8:]
	body[0] = h.Codec
	if h.Extended() {
		body[1] = FrameVersionExtended
		binary.LittleEndian.PutUint16(body[2:], FrameExtensionSize)
		ext := dst[FrameHeaderSize : FrameHeaderSize+FrameExtensionSize]
		copy(ext[0:16], h.Producer[:])
		binary.LittleEndian.PutUint64(ext[16:], h.Sequence)
		binary.LittleEndian.PutUint32(ext[24:], h.WriteEntries)
		binary.LittleEndian.PutUint32(ext[28:], 0)
		binary.LittleEndian.PutUint64(ext[32:], h.DictID)
		binary.LittleEndian.PutUint32(ext[40:], crc32.Checksum(ext[:40], crc32q))
	} else {
		body[1] = FrameVersion
		binary.LittleEndian.PutUint16(body[2:], 0)
	}
	binary.LittleEndian.PutUint64(body[4:], uint64(h.FirstOffset))
	binary.LittleEndian.PutUint32(body[12:], h.EntryCount)
	binary.LittleEndian.PutUint32(body[16:], h.StoredSize)
	binary.LittleEndian.PutUint32(body[20:], h.PlainSize)
	binary.LittleEndian.PutUint32(body[24:], h.PayloadCrc)
	binary.LittleEndian.PutUint32(encoded[4:], crc32.Checksum(body, crc32q))
}

// AppendFrameHeader encodes h onto dst and returns the result, the way append does.
func AppendFrameHeader(dst []byte, h FrameHeader) []byte {
	var encoded [FrameHeaderSize + FrameExtensionSize]byte
	PutFrameHeader(encoded[:], h)
	return append(dst, encoded[:h.HeaderSize()]...)
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
	version := body[1]
	if version != FrameVersion && version != FrameVersionExtended {
		return FrameHeader{}, fmt.Errorf("%w: frame version %d, this build reads up to %d",
			ErrUnsupportedLogFormat, version, FrameVersionExtended)
	}
	header := FrameHeader{
		Codec:       body[0],
		FirstOffset: Offset(binary.LittleEndian.Uint64(body[4:])),
		EntryCount:  binary.LittleEndian.Uint32(body[12:]),
		StoredSize:  binary.LittleEndian.Uint32(body[16:]),
		PlainSize:   binary.LittleEndian.Uint32(body[20:]),
		PayloadCrc:  binary.LittleEndian.Uint32(body[24:]),
	}
	if version == FrameVersionExtended {
		if extLen := binary.LittleEndian.Uint16(body[2:]); extLen != FrameExtensionSize {
			// a checked header with an extension this build does not know is from a newer one
			return FrameHeader{}, fmt.Errorf("%w: frame extension of %d bytes, this build reads %d",
				ErrUnsupportedLogFormat, extLen, FrameExtensionSize)
		}
		var ext [FrameExtensionSize]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return FrameHeader{}, err
		}
		if binary.LittleEndian.Uint32(ext[40:]) != crc32.Checksum(ext[:40], crc32q) {
			return FrameHeader{}, fmt.Errorf("%w: header extension checksum mismatch", ErrCorruptFrame)
		}
		copy(header.Producer[:], ext[0:16])
		header.Sequence = binary.LittleEndian.Uint64(ext[16:])
		header.WriteEntries = binary.LittleEndian.Uint32(ext[24:])
		header.DictID = binary.LittleEndian.Uint64(ext[32:])
		if maxStored >= FrameExtensionSize {
			maxStored -= FrameExtensionSize
		} else {
			maxStored = 0
		}
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

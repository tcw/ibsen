package logfmt

import (
	"hash/crc32"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeFrame builds the bytes of one frame: entries, as CreateByteEntry wrote them, put
// through codec behind a header describing what came out. One call to Topic.Write makes one
// frame, which is what makes a frame boundary a flush boundary and a durability boundary:
// the store never sees half a frame, so a flush never lands inside one.
func EncodeFrame(codec driven.Codec, firstOffset domain.Offset, entryCount int, entries []byte) ([]byte, error) {
	if codec == nil {
		codec = driven.NoCodec{}
	}
	if len(entries) > domain.MaxFrameSize {
		return nil, errore.NewF("frame payload of %d bytes exceeds %d", len(entries), domain.MaxFrameSize)
	}
	stored, err := codec.Encode(make([]byte, 0, len(entries)), entries)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	if len(stored) > domain.MaxFrameSize {
		return nil, errore.NewF("frame of %d stored bytes exceeds %d", len(stored), domain.MaxFrameSize)
	}
	header := domain.FrameHeader{
		Codec:       uint8(codec.ID()),
		FirstOffset: firstOffset,
		EntryCount:  uint32(entryCount),
		StoredSize:  uint32(len(stored)),
		PlainSize:   uint32(len(entries)),
		PayloadCrc:  crc32.Checksum(stored, crcTable),
	}
	frame := domain.AppendFrameHeader(make([]byte, 0, domain.FrameHeaderSize+len(stored)), header)
	return append(frame, stored...), nil
}

// DecodeFrame returns the entry bytes a frame holds. The payload must already have been
// checked against the header's checksum, so a failure here is the codec's, not the media's.
func DecodeFrame(codecs driven.Codecs, h domain.FrameHeader, payload []byte) ([]byte, error) {
	codec, err := codecs.Get(driven.CodecID(h.Codec))
	if err != nil {
		return nil, errore.Wrap(err)
	}
	entries, err := codec.Decode(make([]byte, 0, h.PlainSize), payload, int(h.PlainSize))
	if err != nil {
		return nil, errore.Wrap(err)
	}
	if len(entries) != int(h.PlainSize) {
		return nil, errore.NewF("codec %s returned %d bytes, frame says %d",
			driven.CodecID(h.Codec), len(entries), h.PlainSize)
	}
	return entries, nil
}

// ReadFrameHeaderAt reads and verifies the header of the frame starting at byteOffset in a
// log block, without touching its payload.
func ReadFrameHeaderAt(store driven.BlockStore, ref driven.BlockRef, byteOffset int64) (domain.FrameHeader, error) {
	block, err := store.Open(ref, byteOffset)
	if err != nil {
		return domain.FrameHeader{}, errore.Wrap(err)
	}
	defer block.Close()
	header, err := domain.ReadFrameHeader(block, domain.MaxFrameSize)
	if err != nil {
		return domain.FrameHeader{}, errore.Wrap(err)
	}
	return header, nil
}

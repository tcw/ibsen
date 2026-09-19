package logfmt

import (
	"hash/crc32"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// AppendFrame appends one frame to dst and returns the result, the way append does: the
// entries as AppendEntry wrote them, put through codec, behind a header describing what came
// out. One call to Topic.Write makes one frame, which is what makes a frame boundary a flush
// boundary and a durability boundary: the store never sees half a frame, so a flush never
// lands inside one.
//
// dst is the buffer the block will be appended from, so a frame is built where it is going to
// be written rather than assembled beside it and copied in. The header's room is reserved,
// the payload goes in after it, and the header is written last, once its stored size and
// checksum are known. A codec encodes into dst directly for the same reason. Nothing is
// appended when this returns an error, so dst is left as it was.
//
// Compression that did not pay is thrown away. A codec is offered the entries and is taken up
// on it only if what comes back is smaller; otherwise the frame stores the entries as they
// are and its header says so. Costing a codec nothing is not the same as costing it nothing
// to try: a frame too small for a compressor to find anything in comes back larger than it
// went in, and without this a topic could be made bigger by turning compression on. A tie
// goes to the plain bytes, which are cheaper to read and readable by a build that does not
// carry the codec at all.
//
// This is per frame, so it costs nothing to get wrong at the topic level: a topic that
// batches well compresses, and the same topic's occasional single-entry write does not.
func AppendFrame(dst []byte, codec driven.Codec, firstOffset domain.Offset, entryCount int, entries []byte) ([]byte, error) {
	if codec == nil {
		codec = driven.NoCodec{}
	}
	start := len(dst)
	if len(entries) > domain.MaxFrameSize {
		return dst, errore.NewF("frame payload of %d bytes exceeds %d", len(entries), domain.MaxFrameSize)
	}
	dst = append(dst, make([]byte, domain.FrameHeaderSize)...)
	payloadStart := len(dst)

	codecID := codec.ID()
	if codecID == driven.CodecNone {
		// there is nothing to put the entries through, so they are the payload
		dst = append(dst, entries...)
	} else {
		var err error
		dst, err = codec.Encode(dst, entries)
		if err != nil {
			return dst[:start], errore.Wrap(err)
		}
		if len(dst)-payloadStart >= len(entries) {
			codecID = driven.CodecNone
			dst = append(dst[:payloadStart], entries...)
		}
	}

	stored := dst[payloadStart:]
	if len(stored) > domain.MaxFrameSize {
		return dst[:start], errore.NewF("frame of %d stored bytes exceeds %d", len(stored), domain.MaxFrameSize)
	}
	domain.PutFrameHeader(dst[start:], domain.FrameHeader{
		Codec:       uint8(codecID),
		FirstOffset: firstOffset,
		EntryCount:  uint32(entryCount),
		StoredSize:  uint32(len(stored)),
		PlainSize:   uint32(len(entries)),
		PayloadCrc:  crc32.Checksum(stored, crcTable),
	})
	return dst, nil
}

// DecodeFrame returns the entry bytes a frame holds. The payload must already have been
// checked against the header's checksum, so a failure here is the codec's, not the media's.
//
// A frame written without a codec is its entries as they are, and the returned bytes are
// then the payload itself rather than a copy of it: the read filled that buffer for this
// frame and nothing reuses it, so copying it would be a copy of every byte read. The entries
// a read hands on point into whichever buffer comes back here, and it must not be changed
// while they are in use.
func DecodeFrame(codecs driven.Codecs, h domain.FrameHeader, payload []byte) ([]byte, error) {
	if driven.CodecID(h.Codec) == driven.CodecNone {
		if len(payload) != int(h.PlainSize) {
			return nil, errore.NewF("stored payload is %d bytes, frame says %d plain",
				len(payload), h.PlainSize)
		}
		return payload, nil
	}
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

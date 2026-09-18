package index

import (
	"bufio"
	"errors"
	"io"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/errore"
)

// ErrInvalidSparsity is returned for a sparsity of zero, which has no meaning: every entry
// offset would have to be a multiple of nothing.
var ErrInvalidSparsity = errors.New("index sparsity must be at least 1")

// CreateBinaryIndexFromLog scans a log block from a reader positioned at fromByteOffset,
// which must be a frame boundary, and returns (offset, byteOffset) pairs pointing at the
// start of frames, together with the byte offset where the scan ended.
//
// A frame earns a pair when it covers an offset that is a multiple of oneEntryForEvery. A
// frame is the smallest thing a read can be aimed at, so pointing inside one would buy
// nothing: with a single entry to a frame this indexes exactly the offsets the entry-wise
// rule it replaced did, and with frames larger than the sparsity it indexes every frame.
// Either way a read scans at most one frame plus the sparsity.
//
// Nothing is decoded. A header says how many bytes its frame takes, so the scan steps over
// payloads it never has to understand, and a codec this build lacks costs indexing nothing.
func CreateBinaryIndexFromLog(logBlock io.Reader, fromByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	if oneEntryForEvery == 0 {
		return nil, fromByteOffset, errore.Wrap(ErrInvalidSparsity)
	}
	sparsity := uint64(oneEntryForEvery)
	var pairs []byte
	byteOffset := fromByteOffset
	reader := bufio.NewReader(logBlock)
	for {
		header, err := domain.ReadFrameHeader(reader, domain.MaxFrameSize)
		if err == io.EOF {
			return pairs, byteOffset, nil
		}
		if err != nil {
			return nil, byteOffset, errore.Wrap(err)
		}
		if err = domain.SkipFramePayload(reader, header); err != nil {
			return nil, byteOffset, errore.Wrap(err)
		}
		if coversMultipleOf(header, sparsity) {
			pairs = AppendPair(pairs, domain.OffsetFilePtr{
				Offset:     header.FirstOffset,
				ByteOffset: byteOffset,
			})
		}
		byteOffset = byteOffset + header.Size()
	}
}

// coversMultipleOf reports whether any offset the frame holds is a multiple of sparsity.
func coversMultipleOf(header domain.FrameHeader, sparsity uint64) bool {
	first := uint64(header.FirstOffset)
	multiple := first
	if remainder := first % sparsity; remainder != 0 {
		multiple = first + (sparsity - remainder)
	}
	return multiple < uint64(header.EndOffset())
}

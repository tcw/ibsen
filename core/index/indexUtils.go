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
// which must be an entry boundary, and returns (offset, byteOffset) pairs for every entry
// whose offset is a multiple of oneEntryForEvery, together with the byte offset where the
// scan ended. A oneEntryForEvery of 1 indexes every entry.
func CreateBinaryIndexFromLog(logBlock io.Reader, fromByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	if oneEntryForEvery == 0 {
		return nil, fromByteOffset, errore.Wrap(ErrInvalidSparsity)
	}
	var pairs []byte
	byteOffset := fromByteOffset
	reader := bufio.NewReader(logBlock)
	for {
		entry, n, err := domain.ReadEntry(reader, domain.MaxEntrySize)
		if err == io.EOF {
			return pairs, byteOffset, nil
		}
		if err != nil {
			return nil, byteOffset, errore.Wrap(err)
		}
		if entry.Offset%uint64(oneEntryForEvery) == 0 {
			pairs = AppendPair(pairs, domain.OffsetFilePtr{
				Offset:     domain.Offset(entry.Offset),
				ByteOffset: byteOffset,
			})
		}
		byteOffset = byteOffset + int64(n)
	}
}

package index

import (
	"bufio"
	"io"

	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
)

// CreateBinaryIndexFromLog scans a log block from a reader positioned at fromByteOffset,
// which must be an entry boundary, and returns (offset, byteOffset) pairs for every entry
// whose offset is a multiple of oneEntryForEvery, together with the byte offset where the
// scan ended.
func CreateBinaryIndexFromLog(logBlock io.Reader, fromByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	var index []uint64
	byteOffset := fromByteOffset
	reader := bufio.NewReader(logBlock)
	for {
		entry, n, err := common.ReadEntry(reader, common.MaxEntrySize)
		if err == io.EOF {
			return common.Uint64ArrayToBytes(index), byteOffset, nil
		}
		if err != nil {
			return nil, byteOffset, errore.Wrap(err)
		}
		if entry.Offset%uint64(oneEntryForEvery) == 0 {
			index = append(index, entry.Offset, uint64(byteOffset))
		}
		byteOffset = byteOffset + int64(n)
	}
}

package index

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
	"io"
)

// CreateBinaryIndexFromLogFile scans a log block from logfileByteOffset, which must be an
// entry boundary, and returns (offset, byteOffset) pairs for every entry whose offset is a
// multiple of oneEntryForEvery, together with the byte offset where the scan ended.
func CreateBinaryIndexFromLogFile(afs *afero.Afero, logFileName string, logfileByteOffset int64, oneEntryForEvery uint32) ([]byte, int64, error) {
	file, err := common.OpenFileForRead(afs, logFileName)
	if err != nil {
		return nil, 0, errore.Wrap(err)
	}
	defer file.Close()
	if logfileByteOffset > 0 {
		if _, err = file.Seek(logfileByteOffset, io.SeekStart); err != nil {
			return nil, logfileByteOffset, errore.Wrap(err)
		}
	}
	var index []uint64
	byteOffset := logfileByteOffset
	reader := bufio.NewReader(file)
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

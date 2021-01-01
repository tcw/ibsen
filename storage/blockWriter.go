package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
)

type BlockWriterParams struct {
	Afs       *afero.Afero
	Filename  string
	LogEntry  [][]byte
	offset    uint64
	blockSize int64
}

func (bw BlockWriterParams) WriteBatch() (uint64, int64, error) {
	writer, err := OpenFileForWrite(bw.Afs, bw.Filename)
	if err != nil {
		return bw.offset, bw.blockSize, errore.WrapWithContext(err)
	}
	offset, blockSize, err := bw.writeBatchToFile(writer)
	if err != nil {
		return offset, blockSize, errore.WrapWithContext(err)
	}
	err = writer.Close()
	if err != nil {
		return offset, blockSize, errore.WrapWithContext(err)
	}
	return offset, blockSize, nil
}

func (bw BlockWriterParams) writeBatchToFile(file afero.File) (uint64, int64, error) {
	var bytes []byte
	offset := bw.offset
	size := bw.blockSize
	for _, entry := range bw.LogEntry {
		offset = offset + 1
		bytes = append(bytes, createByteEntry(entry, offset)...)
	}
	n, err := file.Write(bytes)
	size = size + int64(n)
	if err != nil {
		return offset, size, errore.WrapWithContext(err)
	}
	return offset, size, nil
}

func createByteEntry(entry []byte, currentOffset uint64) []byte {
	offset := uint64ToLittleEndian(currentOffset)
	byteSize := intToLittleEndian(len(entry))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := uint32ToLittleEndian(checksum)
	bytes := append(offset, check...)
	bytes = append(bytes, byteSize...)
	bytes = append(bytes, entry...)
	return bytes
}

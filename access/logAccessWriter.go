package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

type TopicWriter struct {
	Afs              *afero.Afero
	Topic            Topic
	CurrentOffset    Offset
	CurrentBlockSize BlockSizeInBytes
}

func (tw *TopicWriter) clearCurrentBlockSize() {
	tw.CurrentBlockSize = 0
}

func (tw *TopicWriter) update(offset Offset, blockSize BlockSizeInBytes) {
	tw.CurrentOffset = offset
	tw.CurrentBlockSize = blockSize
}

func (tw *TopicWriter) Write(fileName FileName, entries Entries) error {

	writer, err := commons.OpenFileForWrite(tw.Afs, string(fileName))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = tw.writeBatchToFile(writer, entries)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = writer.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (tw *TopicWriter) writeBatchToFile(file afero.File, entries Entries) error {
	var bytes []byte

	for _, entry := range entries {
		bytes = append(bytes, createByteEntry(entry, tw.CurrentOffset)...)
		tw.CurrentOffset = tw.CurrentOffset + 1
	}
	n, err := file.Write(bytes)
	tw.CurrentBlockSize = BlockSizeInBytes(uint64(tw.CurrentBlockSize) + uint64(n))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func createByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := commons.Uint64ToLittleEndian(uint64(currentOffset))
	byteSize := commons.IntToLittleEndian(len(entry))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := commons.Uint32ToLittleEndian(checksum)
	bytes := append(offset, check...)
	bytes = append(bytes, byteSize...)
	bytes = append(bytes, entry...)
	return bytes
}

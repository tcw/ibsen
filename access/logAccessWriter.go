package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"sync"
)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

type TopicWriter struct {
	Afs              *afero.Afero
	mu               *sync.Mutex
	Topic            commons.Topic
	CurrentOffset    commons.Offset
	CurrentBlockSize commons.BlockSizeInBytes
}

func (tw *TopicWriter) Write(fileName string, entries commons.Entries) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	writer, err := commons.OpenFileForWrite(tw.Afs, fileName)
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

func (tw *TopicWriter) writeBatchToFile(file afero.File, entries commons.Entries) error {
	var bytes []byte

	for _, entry := range entries {
		bytes = append(bytes, createByteEntry(entry, tw.CurrentOffset)...)
		tw.CurrentOffset = tw.CurrentOffset + 1
	}
	n, err := file.Write(bytes)
	tw.CurrentBlockSize = commons.BlockSizeInBytes(uint64(tw.CurrentBlockSize) + uint64(n))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func createByteEntry(entry []byte, currentOffset commons.Offset) []byte {
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

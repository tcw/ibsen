package common

import (
	"encoding/binary"
	"errors"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/utils"
	"hash/crc32"
	"os"
)

const Sep = string(os.PathSeparator)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

var FileNotFound = errors.New("file not found")

func MemAfs() *afero.Afero {
	var fs = afero.NewMemMapFs()
	return &afero.Afero{Fs: fs}
}

func OpenFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return f, nil
}

func OpenFileForRead(afs *afero.Afero, fileName string) (afero.File, error) {
	exists, err := afs.Exists(fileName)
	if err != nil {
		return nil, errore.NewF("Failes checking if file %s exist", fileName)
	}
	if !exists {
		return nil, FileNotFound
	}
	f, err := afs.OpenFile(fileName, os.O_RDONLY, 0400)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	return f, nil
}

func CreateByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := Uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := Uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(byteSize, crc32q)
	checksum = crc32.Update(checksum, crc32q, entry)
	checksum = crc32.Update(checksum, crc32q, offset)
	check := Uint32ToLittleEndian(checksum)
	return utils.JoinSize(20+entrySize, check, byteSize, entry, offset)
}

func Uint64ArrayToBytes(uintArray []uint64) []byte {
	var bytes []byte
	for _, value := range uintArray {
		bytes = append(bytes, Uint64ToLittleEndian(value)...)
	}
	return bytes
}

func Uint64ToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func Uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

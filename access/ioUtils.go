package access

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

func openFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func OpenFileForRead(afs *afero.Afero, fileName string) (afero.File, error) {
	exists, err := afs.Exists(fileName)
	if err != nil {
		return nil, errore.NewWithContext(fmt.Sprintf("Failes checking if file %s exist", fileName))
	}
	if !exists {
		return nil, errore.NewWithContext(fmt.Sprintf("File %s does not exist", fileName))
	}
	f, err := afs.OpenFile(fileName, os.O_RDONLY, 0400)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func listFilesInDirectory(afs *afero.Afero, dir string, fileExtension string) ([]string, error) {
	filenames := make([]string, 0)
	file, err := OpenFileForRead(afs, dir)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	names, err := file.Readdirnames(0)
	for _, name := range names {
		hasSuffix := strings.HasSuffix(name, fileExtension)
		if hasSuffix {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func ListAllFilesInTopic(afs *afero.Afero, rootPath string, topic string) ([]os.FileInfo, error) {
	dir, err := OpenFileForRead(afs, rootPath+Sep+topic)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer dir.Close()
	return dir.Readdir(0)
}

func filesToBlocks(paths []string) ([]Block, error) {
	blocks := make([]Block, 0)
	for _, path := range paths {
		_, file := filepath.Split(path)
		ext := filepath.Ext(file)
		fileNameStem := strings.TrimSuffix(file, ext)
		mod, err := strconv.ParseUint(fileNameStem, 10, 64)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		blocks = append(blocks, Block(mod))
	}
	return blocks, nil
}

func listAllTopics(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer file.Close()
	names, err := file.Readdirnames(0)
	for _, name := range names {
		isHidden := strings.HasPrefix(name, ".")
		if !isHidden {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func fastForwardToOffset(file afero.File, offset Offset, currentOffset Offset) error {
	var offsetFound Offset = math.MaxInt64
	for {
		if offsetFound == currentOffset {
			return io.EOF
		}
		if offsetFound+1 == offset {
			return nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return err
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		offsetFound = Offset(littleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)
		_, err = (file).Seek(int64(size), 1)
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
}

func ReadOffset(afs *afero.Afero, name FileName, startAtByteOffset int64) (Offset, error) {
	file, err := OpenFileForRead(afs, string(name))
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if startAtByteOffset > 0 {
		_, err = file.Seek(startAtByteOffset, io.SeekStart)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}
	bytes := make([]byte, 8)

	_, err = io.ReadFull(file, bytes)
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return Offset(littleEndianToUint64(bytes)), nil
}

func FindByteOffsetFromOffset(afs *afero.Afero, fileName FileName, startAtByteOffset int64, offset Offset) (int64, error) {
	file, err := OpenFileForRead(afs, string(fileName))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if startAtByteOffset > 0 {
		_, err = file.Seek(startAtByteOffset, io.SeekStart)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)

	var offsetInFile int64 = math.MaxInt64
	var byteOffset = startAtByteOffset
	for {
		offsetBytes, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			return 0, err
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		if Offset(offsetInFile+1) == offset {
			return byteOffset, nil
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		offsetInFile = int64(littleEndianToUint64(bytes))

		checksumBytes, err := io.ReadFull(reader, checksum)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		sizeBytes, err := io.ReadFull(reader, bytes)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		entrySize := littleEndianToUint64(bytes)
		entryBytes := make([]byte, entrySize)
		_, err = io.ReadFull(reader, entryBytes)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		byteOffset = byteOffset + int64(offsetBytes) + int64(checksumBytes) + int64(sizeBytes) + int64(entrySize)
	}
}

func FileSize(asf *afero.Afero, fileName string) (int64, error) {
	exists, err := asf.Exists(fileName)
	if err != nil {
		return 0, errore.NewWithContext(fmt.Sprintf("Failes checking if file %s exist", fileName))
	}
	if !exists {
		return 0, errore.NewWithContext(fmt.Sprintf("File %s does not exist", fileName))
	}

	file, err := asf.OpenFile(fileName,
		os.O_RDONLY, 0400)
	fi, err := file.Stat()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	err = file.Close()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return fi.Size(), nil
}

func FindLastOffset(afs *afero.Afero, blockFileName FileName, from int64) (int64, error) {
	var offsetFound int64 = 0
	file, err := OpenFileForRead(afs, string(blockFileName))
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	defer file.Close()
	_, err = file.Seek(from, io.SeekStart)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	for {
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return offsetFound, nil
		}
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		offsetFound = int64(littleEndianToUint64(bytes))
		_, err = io.ReadFull(file, checksum)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		_, err = io.ReadFull(file, bytes)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
		size := littleEndianToUint64(bytes)
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			return offsetFound, errore.WrapWithContext(err)
		}
	}
}

func ReadFile(file afero.File, logChan chan *[]LogEntry, wg *sync.WaitGroup, batchSize uint32, byteOffset int64) error {
	_, err := file.Seek(byteOffset, io.SeekStart)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	checksum := make([]byte, 4)
	logEntries := make([]LogEntry, batchSize)
	slicePointer := 0
	for {
		if slicePointer != 0 && uint32(slicePointer)%batchSize == 0 {
			wg.Add(1)
			sendingEntries := logEntries[:slicePointer]
			logChan <- &sendingEntries
			logEntries = make([]LogEntry, batchSize)
			slicePointer = 0
		}
		_, err := io.ReadFull(reader, bytes)
		if err == io.EOF {
			if logEntries != nil && slicePointer > 0 {
				wg.Add(1)
				sendingEntries := logEntries[:slicePointer]
				logChan <- &sendingEntries
			}
			return nil
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}

		offset := int64(littleEndianToUint64(bytes))
		_, err = io.ReadFull(reader, checksum)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		checksumValue := littleEndianToUint32(bytes)
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		size := littleEndianToUint64(bytes)

		entry := make([]byte, size)

		_, err = io.ReadFull(reader, entry)
		if err != nil {
			return errore.WrapWithContext(err)
		}

		logEntries[slicePointer] = LogEntry{
			Offset:   uint64(offset),
			Crc:      checksumValue,
			ByteSize: int(size),
			Entry:    entry,
		}
		slicePointer = slicePointer + 1
	}
}

func MarshallIndex(soi []byte) (Index, error) {
	if len(soi) == 0 {
		return Index{}, errors.New("NoBytesInIndex")
	}
	var numberPart []byte
	var offset uint64
	isOffset := true
	var index = Index{
		IndexOffsets: nil,
	}
	for _, byteValue := range soi {
		numberPart = append(numberPart, byteValue)
		if !isLittleEndianMSBSet(byteValue) {
			value, n := proto.DecodeVarint(numberPart)
			if n < 0 {
				return Index{}, errore.NewWithContext("Vararg returned negative numberPart, indicating a parsing error")
			}
			if isOffset {
				offset = value
				isOffset = false
			} else {
				index.add(IndexOffset{
					Offset:     Offset(offset),
					ByteOffset: int64(value),
				})
				isOffset = true
			}
			numberPart = make([]byte, 0)
		}
	}
	return index, nil
}

func createByteEntry(entry []byte, currentOffset Offset) []byte {
	offset := uint64ToLittleEndian(uint64(currentOffset))
	entrySize := len(entry)
	byteSize := uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(offset, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := uint32ToLittleEndian(checksum)
	return JoinSize(20+entrySize, offset, check, byteSize, entry)
}

func isLittleEndianMSBSet(byteValue byte) bool {
	return (byteValue>>7)&1 == 1
}

func uint64ToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

func uint16ToLittleEndian(number uint16) []byte {
	bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(bytes, number)
	return bytes
}

func littleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func littleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

func littleEndianToUint16(bytes []byte) uint16 {
	return binary.LittleEndian.Uint16(bytes)
}

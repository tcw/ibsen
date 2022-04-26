package access

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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

func createByteEntry(entry []byte, offset Offset) []byte {
	offsetAsByte := uint64ToLittleEndian(uint64(offset))
	entrySize := len(entry)
	byteSize := uint64ToLittleEndian(uint64(entrySize))
	checksum := crc32.Checksum(offsetAsByte, crc32q)
	checksum = crc32.Update(checksum, crc32q, byteSize)
	checksum = crc32.Update(checksum, crc32q, entry)
	check := uint32ToLittleEndian(checksum)
	return joinSize(20+entrySize, offsetAsByte, check, byteSize, entry)
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

func ReadOffset(afs *afero.Afero, name string, startAtByteOffset int64) (Offset, error) {
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

func FindByteOffsetFromOffset(afs *afero.Afero, fileName string, startAtByteOffset int64, offset Offset) (int64, error) {
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

func FindLastOffset(afs *afero.Afero, blockFileName string, from int64) (int64, error) {
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
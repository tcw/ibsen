package access

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
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
	f, err := afs.OpenFile(fileName,
		os.O_RDONLY, 0400)
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

func ListAllFilesInTopic(afs *afero.Afero, rootPath string, topic Topic) ([]os.FileInfo, error) {
	dir, err := OpenFileForRead(afs, rootPath+Sep+string(topic))
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

func listAllTopics(afs *afero.Afero, dir string) ([]Topic, error) {
	var filenames []Topic
	file, err := OpenFileForRead(afs, string(dir))
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer file.Close()
	names, err := file.Readdirnames(0)
	for _, name := range names {
		isHidden := strings.HasPrefix(name, ".")
		if !isHidden {
			filenames = append(filenames, Topic(name))
		}
	}
	return filenames, nil
}

func fastForwardToOffset(file afero.File, offset Offset, lastWrittenOffset Offset) error { //Todo: replace with index
	var offsetFound Offset = math.MaxInt64
	for {
		if offsetFound == lastWrittenOffset {
			return io.EOF
		}
		if offsetFound == offset {
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
		if Offset(offsetInFile) == offset {
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

func intToLittleEndian(number int) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(number))
	return bytes
}

func littleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func littleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

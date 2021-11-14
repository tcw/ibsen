package access

import (
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

const Separator = string(os.PathSeparator)

func openFileForReadWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func openFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func openFileForRead(afs *afero.Afero, fileName string) (afero.File, error) {
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
	var filenames []string
	file, err := openFileForRead(afs, dir)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	names, err := file.Readdirnames(0)
	for _, name := range names {
		hasSuffix := strings.HasSuffix(name, "."+fileExtension)
		if hasSuffix {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func filesToBlocks(paths []string) ([]Block, error) {
	var blocks []Block
	for _, path := range paths {
		_, file := filepath.Split(path)
		ext := filepath.Ext(file)
		fileNameStem := strings.TrimSuffix(file, ext)
		blockMod := strings.Split(fileNameStem, "_")
		mod, err := strconv.ParseUint(blockMod[0], 10, 64)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		blocks = append(blocks, Block(mod))
	}
	return blocks, nil
}

func listAllTopics(afs *afero.Afero, dir string) ([]Topic, error) {
	var filenames []Topic
	file, err := openFileForRead(afs, string(dir))
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

func blockSize(asf *afero.Afero, fileName string) (int64, error) {
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

func findLastOffset(afs *afero.Afero, blockFileName FileName) (int64, error) {
	var offsetFound int64 = 0
	file, err := openFileForRead(afs, string(blockFileName))
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	defer file.Close()
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

func fastForwardToOffset(file afero.File, offset Offset) error { //Todo: replace with index
	var offsetFound Offset = math.MaxInt64
	for {
		if offsetFound == offset {
			return nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		_, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return errore.NewWithContext("no Offset in block")
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
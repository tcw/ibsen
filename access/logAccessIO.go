package access

import (
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const Separator = string(os.PathSeparator)

func OpenFileForReadWrite(afs *afero.Afero, fileName string) (afero.File, error) {
	f, err := afs.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return f, nil
}

func OpenFileForWrite(afs *afero.Afero, fileName string) (afero.File, error) {
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

func ListFilesInDirectory(afs *afero.Afero, dir string, fileExtension string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
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

func FilesToBlocks(paths []string) ([]commons.Offset, error) {
	var blocks []commons.Offset
	for _, path := range paths {
		_, file := filepath.Split(path)
		ext := filepath.Ext(file)
		fileNameStem := strings.TrimSuffix(file, ext)
		blockMod := strings.Split(fileNameStem, "_")
		mod, err := strconv.ParseUint(blockMod[0], 10, 64)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		blocks = append(blocks, commons.Offset(mod))
	}
	return blocks, nil
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

func IntToLittleEndian(number int) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(number))
	return bytes
}

func LittleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func LittleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

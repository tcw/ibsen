package ext4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const separator = string(os.PathSeparator)

func OpenFileForReadWrite(fileName string) (*os.File, error) {
	f, err := os.OpenFile(fileName,
		os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func OpenFileForWrite(fileName string) (*os.File, error) {
	f, err := os.OpenFile(fileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func OpenFileForRead(fileName string) (*os.File, error) {
	if !doesFileExist(fileName) {
		return nil, errors.New(fmt.Sprintf("File %s does not exist", fileName))
	}
	f, err := os.OpenFile(fileName,
		os.O_RDONLY, 0400)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return f, nil
}

func blockSize(filename string) (int64, error) {
	if !doesFileExist(filename) {
		return 0, nil
	}
	file, err := os.OpenFile(filename,
		os.O_RDONLY, 0400)
	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	err = file.Close()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func createBlockFileName(blockName int64) string {
	return fmt.Sprintf("%020d.log", blockName)
}

func doesTopicExist(rootPath string, topicName string) bool {
	_, err := os.Stat(rootPath + separator + topicName)
	return !os.IsNotExist(err)
}

func doesFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func listUnhiddenDirectories(root string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(root)
	if err != nil {
		return files, err
	}
	for _, file := range fileInfo {
		if file.IsDir() {
			_, name := filepath.Split(file.Name())
			isHidden := strings.HasPrefix(name, ".")
			if !isHidden {
				files = append(files, file.Name())
			}
		}
	}
	return files, nil
}

func findLastOffset(blockFileName string) (int64, error) {
	var offsetFound int64 = -1
	file, err := OpenFileForRead(blockFileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	for {
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		n, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return offsetFound, nil
		}

		if err == io.EOF {
			return offsetFound, errors.New("no offset in block")
		}
		if err != nil {
			return offsetFound, errors.New("error")
		}
		if n != 8 {
			log.Println("offset incorrect")
			return offsetFound, errors.New("offset incorrect")
		}
		offsetFound = int64(fromLittleEndian(bytes))
		n, err2 := io.ReadFull(file, checksum)
		if err2 != nil {
			return offsetFound, errors.New("error")
		}
		if n != 4 {
			log.Println("byte size incorrect")
			return offsetFound, errors.New("byte size incorrect")
		}

		n, err3 := io.ReadFull(file, bytes)
		if err3 != nil {
			log.Println(err3)
			return offsetFound, err3
		}
		if n != 8 {
			log.Println("byte size incorrect")
			return offsetFound, errors.New("byte size incorrect")
		}
		size := fromLittleEndian(bytes)
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			println(err)
			return offsetFound, err
		}
	}
}

func fastForwardToOffset(file *os.File, offset int64) error {
	var offsetFound int64 = -1
	for {
		if offsetFound == offset {
			return nil
		}
		bytes := make([]byte, 8)
		checksum := make([]byte, 4)
		n, err := io.ReadFull(file, bytes)
		if err == io.EOF {
			return errors.New("no offset in block")
		}
		if err != nil {
			return errors.New("error")
		}
		if n != 8 {
			log.Println("offset incorrect")
			return errors.New("offset incorrect")
		}
		offsetFound = int64(fromLittleEndian(bytes))
		n, err2 := io.ReadFull(file, checksum)
		if err2 != nil {
			return errors.New("error")
		}
		if n != 4 {
			log.Println("byte size incorrect")
			return errors.New("byte size incorrect")
		}

		n, err3 := io.ReadFull(file, bytes)
		if err3 != nil {
			log.Println(err3)
			return err3
		}
		if n != 8 {
			log.Println("byte size incorrect")
			return errors.New("byte size incorrect")
		}
		size := fromLittleEndian(bytes)
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			println(err)
			return err
		}
	}
}

func listFilesInDirectory(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if path != dir {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if files == nil {
		files = make([]string, 0)
	}
	return files, nil
}

func filesToBlocks(files []string) ([]int64, error) {
	var blocks []int64
	for _, file := range files {
		splitFileName := strings.Split(file, ".")
		if len(splitFileName) != 2 {
			continue
		}
		if splitFileName[1] == "log" {
			splitPath := strings.Split(splitFileName[0], separator)
			parseUint, err := strconv.ParseInt(splitPath[len(splitPath)-1], 10, 64)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, parseUint)
		}
	}
	return blocks, nil
}

func offsetToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func byteSizeToLittleEndian(number int) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(number))
	return bytes
}

func uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

func fromLittleEndian(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func fromLittleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

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
	"sort"
	"strconv"
	"strings"
)

const separator = string(os.PathSeparator)

func findCurrentOffset(blockFileName string) (uint64, error) {
	var offsetFound uint64 = 0
	file, err := os.OpenFile(blockFileName,
		os.O_RDONLY, 0400)
	defer file.Close()
	if err != nil {
		return 0, err
	}
	for {
		bytes := make([]byte, 8)
		n, err := file.Read(bytes)
		if err == io.EOF {
			return offsetFound, nil
		}
		if err != nil {
			return offsetFound, errors.New("error")
		}
		if n != 8 {
			log.Println("offset incorrect")
		}
		offsetFound = fromLittleEndian(bytes)

		n, err2 := file.Read(bytes)
		if n != 8 {
			log.Println("offset incorrect")
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			log.Println(err2)
			return 0, err2
		}
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			println(err)
			return 0, err
		}
	}
}

func blockSizeFromFilename(filename string) (int64, error) {
	file, err := os.OpenFile(filename,
		os.O_RDONLY, 0400)
	fi, err := file.Stat()
	if err != nil {
		log.Println(err)
	}
	err = file.Close()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func blockSize(file *os.File) int64 {
	fi, err := file.Stat()
	if err != nil {
		log.Println(err)
	}
	return fi.Size()
}

func createBlockFileName(blockName int64) string {
	return fmt.Sprintf("%020d.log", blockName)
}

func doesTopicExist(rootPath string, topicName string) bool {
	_, err := os.Stat(rootPath + separator + topicName)
	return !os.IsNotExist(err)
}

func createTopic(rootPath string, topicName string) (bool, error) {
	exist := doesTopicExist(rootPath, topicName)
	if exist {
		return false, nil
	}
	err := os.Mkdir(rootPath+separator+topicName, 0777) //Todo: more restrictive
	if err != nil {
		return false, err
	}
	return true, nil
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

func listFilesInDirectory(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func listBlocksSorted(topicPath string) ([]int64, error) {

	var blocks []int64
	files, err := listFilesInDirectory(topicPath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	for _, file := range files {
		splitFileName := strings.Split(file, ".")
		if len(splitFileName) != 2 {
			continue
		}
		if splitFileName[1] == "log" {
			splitPath := strings.Split(splitFileName[0], separator)
			parseUint, err := strconv.ParseInt(splitPath[len(splitPath)-1], 10, 64)
			if err != nil {
				log.Println("Invalid block format")
			}
			blocks = append(blocks, parseUint)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
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

func fromLittleEndian(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)

}

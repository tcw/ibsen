package ext4

import (
	"encoding/binary"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const separator = string(os.PathSeparator)

func blockSize(file *os.File) int64 {
	fi, err := file.Stat()
	if err != nil {
		log.Println(err)
	}
	return fi.Size()
}

func createBlockFileName(blockName uint64) string {
	return fmt.Sprintf("%020d.log", blockName)
}

func doesTopicExist(rootPath string, topicName string) bool {
	_, err := os.Stat(rootPath + separator + topicName)
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

func listBlocksSorted(topicPath string) ([]uint64, error) {

	var blocks []uint64
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
			parseUint, err := strconv.ParseUint(splitPath[len(splitPath)-1], 10, 64)
			if err != nil {
				log.Println("Invalid block format")
			}
			blocks = append(blocks, parseUint)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return blocks, nil
}

func (t *TopicWrite) findCurrentBlock(topicPath string) (uint64, error) {
	sorted, err := listBlocksSorted(topicPath)
	if err != nil {
		return 0, err
	}
	return sorted[len(sorted)-1], nil
}

func offsetToLittleEndian(offset logStorage.Offset) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(offset))
	return bytes
}

func byteSizeToLittleEndian(number int) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(number))
	return bytes
}

func toLittleEndian(number uint64) []byte {
	offset := make([]byte, 8)
	binary.LittleEndian.PutUint64(offset, number)
	return offset
}

func fromLittleEndian(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)

}

package ext4

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

func performCorruptionCheck(rootPath string) error {
	topics, err := listUnhiddenDirectories(rootPath)
	if err != nil {
		return err
	}
	for _, v := range topics {
		filesInDirectory, err := listFilesInDirectory(rootPath + separator + v)
		if err != nil {
			return err
		}
		blocks, err := filesToBlocks(filesInDirectory)
		if len(blocks) == 0 {
			continue
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
		lastBlock := blocks[len(blocks)-1]
		blockFileName := createBlockFileName(lastBlock)
		file, err := OpenFileForRead(blockFileName)
		if err != nil {
			return err
		}
		safePoint, err := checkForCorruption(file)
		if err != nil {
			correctFile(file.Name(), safePoint)
		}
	}
	return nil
}

func correctFile(filename string, safePoint int) error {
	corruptFile := strings.Replace(filename, ".log", ".corrupt", -1)
	err2 := os.Rename(filename, corruptFile)
	if err2 != nil {
		return err2
	}
	file, err := OpenFileForRead(corruptFile)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(file.Name(),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	buffer := make([]byte, 4096)
	var totalbytesRead int = 0
	for {
		bytesread, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			} else {
				return err
			}
		}
		totalbytesRead = totalbytesRead + bytesread
		if totalbytesRead > safePoint {
			f.Write(buffer[0 : (totalbytesRead-safePoint)-1])
		} else {
			f.Write(buffer)
		}
	}
	f.Close()
	file.Close()
	return nil
}

func checkForCorruption(file *os.File) (int, error) {
	var currentOffset int64 = -1
	var lastSafePoint int = 0
	for {
		bytes := make([]byte, 8)
		n, err := file.Read(bytes)
		if err == io.EOF {
			return lastSafePoint, nil
		}
		if err != nil {
			return lastSafePoint, err
		}
		if n != 8 {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		offset := int64(fromLittleEndian(bytes))
		if currentOffset != -1 {
			if currentOffset+1 != offset {
				return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
			}
		}
		currentOffset = offset

		n, err2 := file.Read(bytes)
		if n != 8 {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		size := fromLittleEndian(bytes)
		if err2 != nil {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		_, err = file.Seek(int64(size), 1)
		if err != nil {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		lastSafePoint = lastSafePoint + 16 + int(size)
	}
}

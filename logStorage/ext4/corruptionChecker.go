package ext4

import (
	"errors"
	"fmt"
	"hash/crc32"
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
	orgFileName := filename
	corruptFile := strings.Replace(filename, ".log", ".corrupt", -1)
	err := os.Rename(filename, corruptFile)
	if err != nil {
		return err
	}
	file, err := OpenFileForRead(corruptFile)
	if err != nil {
		return err
	}
	correctedFile, err := OpenFileForWrite(orgFileName)
	if err != nil {
		return err
	}
	buffer := make([]byte, 4096)
	var totalBytesRead int = 0
	for {
		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				return nil
			}
		}
		totalBytesRead = totalBytesRead + bytesRead
		if totalBytesRead > safePoint {
			correctedFile.Write(buffer[0:(totalBytesRead - safePoint)])
		} else {
			correctedFile.Write(buffer)
		}
	}
	correctedFile.Sync()
	correctedFile.Close()
	file.Close()
	return nil
}

func checkForCorruption(file *os.File) (int, error) {
	var currentOffset int64 = -1
	var lastSafePoint int = 0
	for {
		offsetBytes := make([]byte, 8)
		byteSizeBytes := make([]byte, 8)
		checksum := make([]byte, 4)
		n, err := io.ReadFull(file, offsetBytes)
		if err == io.EOF {
			return lastSafePoint, nil
		}
		if err != nil {
			return lastSafePoint, err
		}
		if n != 8 {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		offset := int64(fromLittleEndian(offsetBytes))
		if currentOffset != -1 {
			if currentOffset+1 != offset {
				return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
			}
		}
		currentOffset = offset

		n, err = io.ReadFull(file, checksum)
		if err != nil {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		if n != 4 {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		checksumValue := fromLittleEndianToUint32(checksum)

		n, err = io.ReadFull(file, byteSizeBytes)
		if err != nil {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		if n != 8 {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		size := fromLittleEndian(byteSizeBytes)

		entryBytes := make([]byte, size)
		n, err4 := io.ReadFull(file, entryBytes)
		if err4 != nil {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		if n != int(size) {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}

		record := append(offsetBytes, byteSizeBytes...)
		record = append(record, entryBytes...)

		recordChecksum := crc32.Checksum(record, crc32q)

		if recordChecksum != checksumValue {
			return lastSafePoint, errors.New(fmt.Sprintf("Detected corruption at offset, illegal checksum %d", currentOffset))
		}

		lastSafePoint = lastSafePoint + 20 + int(size)
	}
}

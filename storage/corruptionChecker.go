package storage

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"hash/crc32"
	"io"
	"sort"
	"strings"
)

func performCorruptionCheck(afs *afero.Afero, rootPath string) error {
	topics, err := commons.ListUnhiddenEntriesDirectory(afs, rootPath)
	if err != nil {
		return err
	}
	for _, v := range topics {
		filesInDirectory, err := commons.ListFilesInDirectory(afs, rootPath+commons.Separator+v, "log")
		if err != nil {
			return err
		}
		blocks, err := filesToBlocks(filesInDirectory)
		if len(blocks) == 0 {
			continue
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
		lastBlock := blocks[len(blocks)-1]
		blockFileName := commons.CreateBlockFileName(lastBlock, "log")
		file, err := commons.OpenFileForRead(afs, blockFileName)
		if err != nil {
			return err
		}
		safePoint, err := checkForCorruption(file)
		if err != nil {
			err := correctFile(afs, file.Name(), safePoint)
			if err != nil {
				return errore.WrapWithContext(err)
			}
		}
	}
	return nil
}

func correctFile(afs *afero.Afero, filename string, safePoint int) error {
	orgFileName := filename
	corruptFile := strings.Replace(filename, ".log", ".corrupt", -1)
	err := afs.Rename(filename, corruptFile)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	file, err := commons.OpenFileForRead(afs, corruptFile)
	defer file.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	correctedFile, err := commons.OpenFileForWrite(afs, orgFileName)
	defer correctedFile.Sync()
	defer correctedFile.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	buffer := make([]byte, 4096)
	var totalBytesRead = 0
	for {
		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return errore.WrapWithContext(err)
			} else {
				return nil
			}
		}
		totalBytesRead = totalBytesRead + bytesRead
		if totalBytesRead > safePoint {
			_, err := correctedFile.Write(buffer[0:(totalBytesRead - safePoint)])
			if err != nil {
				return errore.WrapWithContext(err)
			}
		} else {
			_, err := correctedFile.Write(buffer)
			if err != nil {
				return errore.WrapWithContext(err)
			}
		}
	}
}

func checkForCorruption(file afero.File) (int, error) {
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
			return lastSafePoint, errore.WrapWithContext(err)
		}
		offset := int64(commons.LittleEndianToUint64(offsetBytes))
		if currentOffset != -1 {
			if currentOffset+1 != offset {
				return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
			}
		}
		currentOffset = offset

		n, err = io.ReadFull(file, checksum)
		if err != nil {
			return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		checksumValue := commons.LittleEndianToUint32(checksum)

		n, err = io.ReadFull(file, byteSizeBytes)
		if err != nil {
			return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		size := commons.LittleEndianToUint64(byteSizeBytes)

		entryBytes := make([]byte, size)
		n, err4 := io.ReadFull(file, entryBytes)
		if err4 != nil {
			return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}
		if n != int(size) {
			return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset %d", currentOffset))
		}

		record := append(offsetBytes, byteSizeBytes...)
		record = append(record, entryBytes...)

		recordChecksum := crc32.Checksum(record, crc32q)

		if recordChecksum != checksumValue {
			return lastSafePoint, errore.NewWithContext(fmt.Sprintf("Detected corruption at offset, illegal checksum %d", currentOffset))
		}

		lastSafePoint = lastSafePoint + 20 + int(size)
	}
}

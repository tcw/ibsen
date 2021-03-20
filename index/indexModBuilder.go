package index

import (
	"bufio"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

type Block struct {
	block  uint64
	modulo uint32
}

func parsePath(path string) (Block, error) {
	_, file := filepath.Split(path)
	ext := filepath.Ext(file)
	if ext != ".index" {
		return Block{}, errore.NewWithContext("Index file does not have .index as extension")
	}
	fileNameStem := strings.TrimSuffix(file, ext)
	blockMod := strings.Split(fileNameStem, "_")
	block, err := strconv.ParseUint(blockMod[0], 10, 64)
	if err != nil {
		return Block{}, errore.WrapWithContext(err)
	}
	mod, err := strconv.ParseUint(blockMod[1], 10, 32)
	if err != nil {
		return Block{}, err
	}
	return Block{
		block:  block,
		modulo: uint32(mod),
	}, nil
}

func writeByteOffsetToFile(file afero.File, previousPosition ByteOffset, offsetPositions []ByteOffset) error {
	var bytes []byte
	lastPosition := previousPosition
	for _, position := range offsetPositions {
		bytes = protowire.AppendVarint(bytes, uint64(position-lastPosition))
		lastPosition = position
	}
	_, err := file.Write(bytes)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func readByteOffsetFromFile(afs *afero.Afero, indexFileName string) (map[Offset]ByteOffset, error) {
	indexBlock, err := parsePath(indexFileName)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	modulo := indexBlock.modulo
	file, err := storage.OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var byteValue = make([]byte, 1)
	positions := make(map[Offset]ByteOffset)
	reader := bufio.NewReader(file)
	var counter uint64 = 1
	var number []byte
	var byteOffset uint64
	for {
		_, err := reader.Read(byteValue)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		number = append(number, byteValue[0])
		if !isLittleEndianMSBSet(byteValue[0]) {
			varint, n := protowire.ConsumeVarint(number)
			if n < 0 {
				return nil, errore.NewWithContext("Vararg returned negative number, indicating a parsing error")
			}
			byteOffset = byteOffset + varint
			positions[Offset(counter*uint64(modulo))] = ByteOffset(byteOffset)
			counter = counter + 1
			number = make([]byte, 0)
		}
	}
	return positions, err
}

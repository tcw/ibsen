package index

import (
	"bufio"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
	"math"
	"path/filepath"
	"strconv"
	"strings"
)

type Block struct {
	block  uint64
	modulo uint32
}

type ModuloIndex struct {
	modulo      uint32
	byteOffsets []commons.ByteOffset
}

func (mi ModuloIndex) getClosestByteOffset(offset commons.Offset) (commons.ByteOffset, error) {
	offsetIndex := uint32(math.Floor(float64(offset) / float64(mi.modulo)))
	if int(offsetIndex) > (len(mi.byteOffsets)-1) || offsetIndex < 0 {
		return 0, errore.NewWithContext("Unable to get closest index byte offset")
	}
	return mi.byteOffsets[offsetIndex], nil
}

func (mi ModuloIndex) getByteOffsetHead() commons.ByteOffset {
	return mi.byteOffsets[len(mi.byteOffsets)-1]
}

type TopicModuloIndex struct {
	afs      *afero.Afero
	rootPath string
	topic    string
	modulo   uint32
}

func CreateIndexModBlockFilename(rootPath string, topic string, block uint64, modulo uint32) string {
	filename := fmt.Sprintf("%020d_*%d.%s", block, modulo, "index")
	return rootPath + commons.Separator + topic + commons.Separator + filename
}

func (tmi TopicModuloIndex) BuildIndex(blocks []uint64) {

}

func (tmi TopicModuloIndex) BuildIndexForExistingIndexFile(block uint64, logBlockByteOffset commons.ByteOffset,
	indexLastByteOffset commons.ByteOffset) (commons.ByteOffset, error) {
	logBlockFilename := commons.CreateLogBlockFilename(tmi.rootPath, tmi.topic, block)
	logBlockFile, err := commons.OpenFileForRead(tmi.afs, logBlockFilename)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	_, err = logBlockFile.Seek(int64(logBlockByteOffset), io.SeekStart)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	indexFileName := CreateIndexModBlockFilename(tmi.rootPath, tmi.topic, block, tmi.modulo)
	lastIndexByteOffset := indexLastByteOffset
	if indexLastByteOffset < 0 {
		moduloIndex, err := ReadByteOffsetFromFile(tmi.afs, indexFileName)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		lastIndexByteOffset = moduloIndex.getByteOffsetHead()
	}
	indexFile, err := commons.OpenFileForWrite(tmi.afs, indexFileName)
	for {
		offset, err := storage.ReadOffsetAndByteOffset(logBlockFile, 1000, tmi.modulo)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		if len(offset) == 0 {
			break
		}
		lastIndexByteOffset, err = WriteByteOffsetToFile(indexFile, lastIndexByteOffset, toByteOffset(offset))
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}
	return lastIndexByteOffset, err
}

func (tmi TopicModuloIndex) BuildIndexForNewLogFile(block uint64) (commons.ByteOffset, error) {
	logBlockFilename := commons.CreateLogBlockFilename(tmi.rootPath, tmi.topic, block)
	logBlockFile, err := commons.OpenFileForRead(tmi.afs, logBlockFilename)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	indexFileName := CreateIndexModBlockFilename(tmi.rootPath, tmi.topic, block, tmi.modulo)
	var lastByteOffset commons.ByteOffset = 0
	indexFile, err := commons.OpenFileForWrite(tmi.afs, indexFileName)
	for {
		offset, err := storage.ReadOffsetAndByteOffset(logBlockFile, 1000, tmi.modulo)
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		if len(offset) == 0 {
			break
		}
		lastByteOffset, err = WriteByteOffsetToFile(indexFile, lastByteOffset, toByteOffset(offset))
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
	}
	return lastByteOffset, err
}

func toByteOffset(offsetPositions []storage.OffsetPosition) []commons.ByteOffset {
	var offsets []commons.ByteOffset
	for _, position := range offsetPositions {
		offsets = append(offsets, commons.ByteOffset(position.ByteOffset))
	}
	return offsets
}

func WriteByteOffsetToFile(file afero.File, previousPosition commons.ByteOffset, offsetPositions []commons.ByteOffset) (commons.ByteOffset, error) {
	var bytes []byte
	lastPosition := previousPosition
	for _, position := range offsetPositions {
		bytes = protowire.AppendVarint(bytes, uint64(position-lastPosition))
		lastPosition = position
	}
	_, err := file.Write(bytes)
	if err != nil {
		return lastPosition, errore.WrapWithContext(err)
	}
	return lastPosition, nil
}

func ReadByteOffsetFromFile(afs *afero.Afero, indexFileName string) (ModuloIndex, error) {
	indexBlock, err := parsePath(indexFileName)
	if err != nil {
		return ModuloIndex{}, errore.WrapWithContext(err)
	}
	modulo := indexBlock.modulo
	file, err := commons.OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return ModuloIndex{}, errore.WrapWithContext(err)
	}
	var byteValue = make([]byte, 1)
	positions := make([]commons.ByteOffset, 0)
	reader := bufio.NewReader(file)

	var number []byte
	var byteOffset uint64
	for {
		_, err := reader.Read(byteValue)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ModuloIndex{}, errore.WrapWithContext(err)
		}
		number = append(number, byteValue[0])
		if !isLittleEndianMSBSet(byteValue[0]) {
			nextByteOffset, n := protowire.ConsumeVarint(number)
			if n < 0 {
				return ModuloIndex{}, errore.NewWithContext("Vararg returned negative number, indicating a parsing error")
			}
			byteOffset = byteOffset + nextByteOffset
			positions = append(positions, commons.ByteOffset(byteOffset))
			number = make([]byte, 0)
		}
	}
	return ModuloIndex{
		modulo:      modulo,
		byteOffsets: positions,
	}, nil
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

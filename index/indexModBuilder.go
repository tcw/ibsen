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
	Block  uint64
	Modulo uint32
}

type ModuloIndex struct {
	Modulo      uint32
	ByteOffsets []commons.ByteOffset
}

func (mi ModuloIndex) getClosestByteOffset(offset commons.Offset) (commons.ByteOffset, error) {
	offsetIndex := uint32(math.Floor(float64(offset) / float64(mi.Modulo)))
	if int(offsetIndex) > (len(mi.ByteOffsets)-1) || offsetIndex < 0 {
		return 0, errore.NewWithContext("Unable to get closest index byte offset")
	}
	return mi.ByteOffsets[offsetIndex], nil
}

func (mi ModuloIndex) getByteOffsetHead() commons.ByteOffset {
	return mi.ByteOffsets[len(mi.ByteOffsets)-1]
}

type TopicModuloIndex struct {
	afs      *afero.Afero
	rootPath string
	topic    string
	modulo   uint32
}

func CreateIndexModBlockFilename(rootPath string, topic string, block uint64, modulo uint32) string {
	filename := fmt.Sprintf("%020d_%d.%s", block, modulo, "index")
	return rootPath + commons.Separator + topic + commons.Separator + filename
}

func (tmi TopicModuloIndex) BuildIndex(blocks []uint64, state IndexingState) (IndexingState, error) {
	var currentState IndexingState
	var err error
	for _, block := range blocks {
		indexBlockFilename := commons.CreateIndexBlockFilename(tmi.rootPath, tmi.topic, block)
		fileExist := commons.DoesFileExist(indexBlockFilename)
		if fileExist {
			currentState, err = tmi.BuildIndexForExistingIndexFile(block, state)
		} else {
			currentState, err = tmi.BuildIndexForNewLogFile(block)
		}
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
	}
	return currentState, nil
}

func (tmi TopicModuloIndex) BuildIndexForExistingIndexFile(block uint64, state IndexingState) (IndexingState, error) {
	logBlockFilename := commons.CreateLogBlockFilename(tmi.rootPath, tmi.topic, block)
	indexBlockFileName := CreateIndexModBlockFilename(tmi.rootPath, tmi.topic, block, tmi.modulo)
	logBlockFile, err := commons.OpenFileForRead(tmi.afs, logBlockFilename)
	if err != nil {
		return IndexingState{}, errore.WrapWithContext(err)
	}
	lastLogByteOffset := state.logByteOffset

	if state.IsEmpty() {
		moduloIndex, err := ReadByteOffsetFromFile(tmi.afs, indexBlockFileName)
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
		lastLogByteOffset = moduloIndex.getByteOffsetHead()
	}

	_, err = logBlockFile.Seek(int64(state.logByteOffset), io.SeekStart)
	if err != nil {
		return IndexingState{}, errore.WrapWithContext(err)
	}

	indexBlockFile, err := commons.OpenFileForWrite(tmi.afs, indexBlockFileName)
	var offsetPositions []storage.OffsetPosition
	for {
		offsetPositions, err = storage.ReadOffsetAndByteOffset(logBlockFile, 1000, tmi.modulo)
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
		if len(offsetPositions) == 0 {
			break
		}
		lastLogByteOffset, err = WriteByteOffsetToFile(indexBlockFile, lastLogByteOffset, toByteOffset(offsetPositions))
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
	}
	if len(offsetPositions) > 0 {
		offsetPosition := offsetPositions[len(offsetPositions)-1]
		return IndexingState{
			block:         block,
			logOffset:     commons.Offset(offsetPosition.Offset),
			logByteOffset: commons.ByteOffset(offsetPosition.ByteOffset),
		}, nil
	}
	return state, nil
}

func (tmi TopicModuloIndex) BuildIndexForNewLogFile(block uint64) (IndexingState, error) {
	logBlockFilename := commons.CreateLogBlockFilename(tmi.rootPath, tmi.topic, block)
	indexBlockFileName := CreateIndexModBlockFilename(tmi.rootPath, tmi.topic, block, tmi.modulo)
	logBlockFile, err := commons.OpenFileForRead(tmi.afs, logBlockFilename)
	if err != nil {
		return IndexingState{}, errore.WrapWithContext(err)
	}
	var lastLogByteOffset commons.ByteOffset = 0
	indexBlockFile, err := commons.OpenFileForWrite(tmi.afs, indexBlockFileName)
	var offsetPositions []storage.OffsetPosition
	for {
		offsetPositions, err = storage.ReadOffsetAndByteOffset(logBlockFile, 1000, tmi.modulo)
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
		if len(offsetPositions) == 0 {
			break
		}
		lastLogByteOffset, err = WriteByteOffsetToFile(indexBlockFile, lastLogByteOffset, toByteOffset(offsetPositions))
		if err != nil {
			return IndexingState{}, errore.WrapWithContext(err)
		}
	}
	if len(offsetPositions) > 0 {
		offsetPosition := offsetPositions[len(offsetPositions)-1]
		return IndexingState{
			block:         block,
			logOffset:     commons.Offset(offsetPosition.Offset),
			logByteOffset: commons.ByteOffset(offsetPosition.ByteOffset),
		}, nil
	}
	return IndexingState{}, nil
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
	indexBlock, err := ParsePath(indexFileName)
	if err != nil {
		return ModuloIndex{}, errore.WrapWithContext(err)
	}
	modulo := indexBlock.Modulo
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
		Modulo:      modulo,
		ByteOffsets: positions,
	}, nil
}

func ParsePath(path string) (Block, error) {
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
		Block:  block,
		Modulo: uint32(mod),
	}, nil
}

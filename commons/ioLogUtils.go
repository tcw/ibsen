package commons

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const Separator = string(os.PathSeparator)

var BlockNotFound = errors.New("block not found")

type Offset uint64
type ByteOffset uint64

type IndexedOffset struct {
	Block      uint64
	ByteOffset ByteOffset
}

func (idxOffset IndexedOffset) IsEmpty() bool {
	return idxOffset.ByteOffset == 0
}

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

func CreateBlockFileName(blockName uint64, fileType string) string {
	return fmt.Sprintf("%020d.%s", blockName, fileType)
}

func CreateLogBlockFilename(rootPath string, topic string, block uint64) string {
	return rootPath + Separator + topic + Separator + CreateBlockFileName(block, "log")
}

func CreateIndexBlockFilename(rootPath string, topic string, block uint64) string {
	return rootPath + Separator + topic + Separator + CreateBlockFileName(block, "index")
}

type TopicBlocks struct {
	Topic    string
	FileType string
	Blocks   []uint64
}

func EmptyTopicBlocks(topic string, fileType string) TopicBlocks {
	return TopicBlocks{
		Topic:    topic,
		FileType: fileType,
		Blocks:   make([]uint64, 0),
	}
}

func (tb *TopicBlocks) Size() int {
	return len(tb.Blocks)
}

//BlockHead returns the content of the last block (offset from filename), if
//no the block array is empty it returns BlockNotFound
func (tb *TopicBlocks) BlockHead() (uint64, error) {
	size := tb.Size()
	if size == 0 {
		return 0, BlockNotFound
	}
	return tb.Blocks[size-1], nil
}

func (tb *TopicBlocks) FindBlockContaining(offset Offset) (uint64, error) {
	blockHead, err := tb.BlockHead()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	if uint64(offset) >= blockHead {
		return blockHead, nil
	}
	var lastBlock uint64 = 0
	for _, block := range tb.Blocks {
		if block > uint64(offset) {
			return lastBlock, nil
		}
		lastBlock = block
	}
	return 0, BlockNotFound
}

func (tb *TopicBlocks) LastBlockFileName(rootPath string) (string, error) {
	block, err := tb.BlockHead()
	if err != nil {
		return "", err
	}
	return rootPath + Separator + tb.Topic + Separator + CreateBlockFileName(block, tb.FileType), nil
}

func (tb *TopicBlocks) BlockFilePathsOrderedAsc(rootPath string) []string {
	var blocks []string
	for _, block := range tb.Blocks {
		blocks = append(blocks, rootPath+Separator+tb.Topic+Separator+CreateBlockFileName(block, tb.FileType))
	}
	return blocks
}

func (tb *TopicBlocks) AddBlocks(blocks []uint64) {
	tb.Blocks = append(tb.Blocks, blocks...)
}

func (tb *TopicBlocks) IsEmpty() bool {
	return tb.Blocks == nil || len(tb.Blocks) == 0
}

func ListUnhiddenEntriesDirectory(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	defer file.Close()
	names, err := file.Readdirnames(0)
	for _, name := range names {
		isHidden := strings.HasPrefix(name, ".")
		if !isHidden {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func ListFilesInDirectory(afs *afero.Afero, dir string, filetype string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	names, err := file.Readdirnames(0)
	for _, name := range names {
		hasSuffix := strings.HasSuffix(name, "."+filetype)
		if hasSuffix {
			filenames = append(filenames, name)
		}
	}
	return filenames, nil
}

func ListLogBlocksInTopicOrderedAsc(afs *afero.Afero, rootPath string, topic string) (TopicBlocks, error) {
	return listBlocksInTopicOrderedAsc(afs, rootPath, topic, "log")
}

func ListIndexBlocksInTopicOrderedAsc(afs *afero.Afero, rootPath string, topic string) (TopicBlocks, error) {
	return listBlocksInTopicOrderedAsc(afs, rootPath, topic, "index")
}

func listBlocksInTopicOrderedAsc(afs *afero.Afero, rootPath string, topic string, filetype string) (TopicBlocks, error) {
	var blocks []uint64
	files, err := ListFilesInDirectory(afs, rootPath+Separator+topic, filetype)
	if err != nil {
		return EmptyTopicBlocks(topic, filetype), errore.WrapWithContext(err)
	}
	if len(files) == 0 {
		return TopicBlocks{}, nil
	}
	blocks, err = FilesToBlocks(files)
	if err != nil {
		return EmptyTopicBlocks(topic, filetype), errore.WrapWithContext(err)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return TopicBlocks{
		Topic:  topic,
		Blocks: blocks,
	}, nil
}

func FilesToBlocks(paths []string) ([]uint64, error) {
	var blocks []uint64
	for _, path := range paths {
		_, file := filepath.Split(path)
		ext := filepath.Ext(file)
		if !(ext == ".index" || ext == ".log") {
			continue
		}
		fileNameStem := strings.TrimSuffix(file, ext)
		blockMod := strings.Split(fileNameStem, "_")
		mod, err := strconv.ParseUint(blockMod[0], 10, 64)
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		blocks = append(blocks, mod)
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

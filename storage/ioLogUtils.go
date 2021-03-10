package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"os"
	"sort"
	"strings"
)

const Separator = string(os.PathSeparator)

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

func CreateBlockFileName(blockName int64, fileType string) string {
	return fmt.Sprintf("%020d.%s", blockName, fileType)
}

func CreateLogBlockFilename(rootPath string, topic string, block int64) string {
	return rootPath + Separator + topic + Separator + CreateBlockFileName(block, "log")
}

func CreateIndexBlockFilename(rootPath string, topic string, block int64) string {
	return rootPath + Separator + topic + Separator + CreateBlockFileName(block, "index")
}

type TopicBlocks struct {
	Topic    string
	FileType string
	Blocks   []int64
}

func EmptyTopicBlocks(topic string, fileType string) TopicBlocks {
	return TopicBlocks{
		Topic:    topic,
		FileType: fileType,
		Blocks:   make([]int64, 0),
	}
}

func (tb *TopicBlocks) Size() int {
	return len(tb.Blocks)
}

//LastBlock returns the content of the last block (offset from filename), if
//no the block array is empty it returns BlockNotFound
func (tb *TopicBlocks) LastBlock() (int64, error) {
	size := tb.Size()
	if size == 0 {
		return 0, BlockNotFound
	}
	return tb.Blocks[size-1], nil
}

func (tb *TopicBlocks) LastBlockFileName(rootPath string) (string, error) {
	block, err := tb.LastBlock()
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

func (tb *TopicBlocks) isEmpty() bool {
	return tb.Blocks == nil || len(tb.Blocks) == 0
}

func ListUnhiddenEntriesDirectory(afs *afero.Afero, dir string) ([]string, error) {
	var filenames []string
	file, err := OpenFileForRead(afs, dir)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
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
	var blocks []int64
	files, err := ListFilesInDirectory(afs, rootPath+Separator+topic, filetype)
	if err != nil {
		return EmptyTopicBlocks(topic, filetype), errore.WrapWithContext(err)
	}
	if len(files) == 0 {
		return TopicBlocks{}, nil
	}
	blocks, err = filesToBlocks(files)
	if err != nil {
		return EmptyTopicBlocks(topic, filetype), errore.WrapWithContext(err)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return TopicBlocks{
		Topic:  topic,
		Blocks: blocks,
	}, nil
}

func uint64ToLittleEndian(offset uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, offset)
	return bytes
}

func uint32ToLittleEndian(number uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, number)
	return bytes
}

func intToLittleEndian(number int) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(number))
	return bytes
}

func littleEndianToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func littleEndianToUint32(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}

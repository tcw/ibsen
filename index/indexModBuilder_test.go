package index

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestTopicModuloIndex_BuildIndexForNewLogFile(t *testing.T) {

	const rootPath = "test"
	const topic = "topic1"
	const modulo = 5

	afs := newAfero()

	_, err := createTestLog(afs, rootPath, topic, 0)
	if err != nil {
		t.Fatal(errore.WrapWithContext(err))
	}
	moduloIndex := TopicModuloIndex{
		afs:      afs,
		rootPath: rootPath,
		topic:    topic,
		modulo:   modulo,
	}

	indexState, err := moduloIndex.BuildIndexForNewLogFile(0)
	if err != nil {
		t.Fatal(errore.WrapWithContext(err))
	}
	if indexState.block != 0 {
		t.Fail()
	}
	indexBlockFileName := CreateIndexModBlockFilename(rootPath, topic, 0, modulo)
	readIndex(afs, indexBlockFileName)
	fromFile, err := ReadByteOffsetFromFile(afs, indexBlockFileName)
	offset, err := fromFile.getClosestByteOffset(46)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	if offset != 6000 {
		t.Fail()
	}
}

func Test_writeModToFile(t *testing.T) {
	afs := newAfero()
	file, err := afs.Create("00000000000000000000_5.index")
	if err != nil {
		t.Error(err)
	}
	lastByteOffset, err := WriteByteOffsetToFile(
		file,
		0,
		[]commons.ByteOffset{100, 200})
	if err != nil {
		t.Error(err)
	}
	if lastByteOffset != 200 {
		t.Fail()
	}
	err = file.Close()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(file.Name())
	offsetMap, err := ReadByteOffsetFromFile(afs, file.Name())
	if err != nil {
		t.Error(err)
	}
	fmt.Print(offsetMap)
}

func createTestLog(afs *afero.Afero, rootPath string, topic string, blockName uint64) (string, error) {
	filename := commons.CreateLogBlockFilename(rootPath, topic, blockName)
	writer := storage.BlockWriter{
		Afs:      afs,
		Filename: filename,
		LogEntry: createTestEntries(100, 100),
	}
	offset, _, err := writer.WriteBatch()
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	fmt.Printf("Created log file [%s] with offset head [%d]\n", filename, offset)
	return filename, nil
}

func createTestEntries(entries int, entriesByteSize int) [][]byte {
	var bytes = make([][]byte, 0)

	for i := 0; i < entries; i++ {
		entry := createTestValues(entriesByteSize)
		bytes = append(bytes, entry)
	}
	return bytes
}

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())

	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

func readIndex(afs *afero.Afero, indexFile string) {
	parsePath, err := ParsePath(indexFile)
	if err != nil {
		log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
	}
	modIndex, err := ReadByteOffsetFromFile(afs, indexFile)
	if err != nil {
		log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
	}
	modulo := parsePath.Modulo
	offsets := modIndex.ByteOffsets
	var lastoffset commons.ByteOffset = 0
	for i, offset := range offsets {
		pos := (i * int(modulo)) - 1
		if pos < 0 {
			pos = 0
		}
		fmt.Printf("%d\t%d\t%d\n", pos, offset, offset-lastoffset)
		lastoffset = offset
	}
}

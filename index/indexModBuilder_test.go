package index

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
	"log"
	"testing"
)

func TestTopicModuloIndex_BuildIndexForNewLogFile(t *testing.T) {

	const rootPath = "test"
	const topic = "topic1"
	const modulo = 5

	afs := newAfero()

	_, err := storage.CreateTestLog(afs, rootPath, topic, 0)
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
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	offset, err := fromFile.getClosestByteOffset(49)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	if offset != 6000 {
		t.Fail()
	}
}

func TestTopicModuloIndex_BuildIndexForExistingLogFile(t *testing.T) {

	const rootPath = "test"
	const topic = "topic1"
	const modulo = 5

	afs := newAfero()

	filename, err := storage.CreateTestLog(afs, rootPath, topic, 0)
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

	writer := storage.BlockWriter{
		Afs:       afs,
		Filename:  filename,
		LogEntry:  storage.CreateTestEntries(100, 100),
		Offset:    100,
		BlockSize: 100,
	}
	_, _, err = writer.WriteBatch()
	if err != nil {
		t.Error(err)
	}

	indexState, err = moduloIndex.BuildIndexForExistingIndexFile(indexState)
	if err != nil {
		t.Fatal(errore.WrapWithContext(err))
	}

	indexBlockFileName := CreateIndexModBlockFilename(rootPath, topic, 0, modulo)
	readIndex(afs, indexBlockFileName)
	fromFile, err := ReadByteOffsetFromFile(afs, indexBlockFileName)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	offset, err := fromFile.getClosestByteOffset(149)
	if err != nil {
		t.Fatal(errore.SprintTrace(err))
	}
	if offset != 18000 {
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

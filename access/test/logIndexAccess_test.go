package test

import (
	"encoding/binary"
	"github.com/tcw/ibsen/access"
	"testing"
)

func TestWriteIndexFile(t *testing.T) {
	setUp()
	logIndexAccess := access.ReadWriteLogIndexAccess{
		Afs:          afs,
		RootPath:     rootPath,
		IndexDensity: 0.01,
	}
	var blockList []access.LogBlock
	blockList = append(blockList, 0)
	blocks := access.Blocks{BlockList: blockList}
	testTopic := access.Topic("cars")

	entry := createEntry(1000)
	logFileName := blocks.Head().LogFileName(rootPath, testTopic)
	_, _, err := logAccess.Write(logFileName, entry, 0)
	if err != nil {
		t.Error(err)
	}
	_, err = logIndexAccess.Write(logFileName, 0)
	if err != nil {
		t.Error(err)
	}
	logIndexFileName := blocks.Head().IndexFileName(rootPath, testTopic)
	index, err := logIndexAccess.Read(logIndexFileName)
	if err != nil {
		t.Error(err)
	}
	checkIndexCorrectness(t, index, logFileName)
	indexBlocks, err := logIndexAccess.ReadTopicIndexBlocks(testTopic)
	if err != nil {
		t.Error(err)
	}
	if indexBlocks.BlockList[0] != 0 {
		t.Fail()
		t.Logf("expected %d actural %d", 0, indexBlocks.BlockList[0])
	}
}

func TestWriteIndexFileFromOffset(t *testing.T) {
	setUp()
	logIndexAccess := access.ReadWriteLogIndexAccess{
		Afs:          afs,
		RootPath:     rootPath,
		IndexDensity: 0.1,
	}
	var blockList []access.LogBlock
	blockList = append(blockList, 0)
	blocks := access.Blocks{BlockList: blockList}
	testTopic := access.Topic("cars")

	entry := createEntry(1000)
	logFileName := blocks.Head().LogFileName(rootPath, testTopic)
	offset, byteOffset, err := logAccess.Write(logFileName, entry, 0)
	if err != nil {
		t.Error(err)
	}
	_, err = logIndexAccess.Write(logFileName, 0)
	if err != nil {
		t.Error(err)
	}
	_, _, err = logAccess.Write(logFileName, entry, offset)
	if err != nil {
		t.Error(err)
	}
	_, err = logIndexAccess.Write(logFileName, int64(byteOffset))
	if err != nil {
		t.Error(err)
	}
	indexFileName := blocks.Head().IndexFileName(rootPath, testTopic)
	index, err := logIndexAccess.Read(indexFileName)
	if err != nil {
		t.Error(err)
	}
	checkIndexCorrectness(t, index, logFileName)

}

func checkIndexCorrectness(t *testing.T, index access.Index, logFileName access.FileName) {
	for _, indexOffset := range index.IndexOffsets {
		offsetBytes, err := readBytesFromByteOffset(afs, string(logFileName), indexOffset.ByteOffset, 8)
		if err != nil {
			t.Error(err)
		}
		offsetFromLog := access.Offset(binary.LittleEndian.Uint64(offsetBytes))
		offsetFromIndex := indexOffset.Offset
		if offsetFromLog != offsetFromIndex {
			t.Fail()
			t.Logf("expected %d actual %d", offsetFromIndex, offsetFromLog)
		}
	}
}

package test

import (
	"encoding/binary"
	"github.com/tcw/ibsen/access"
	"testing"
)

func Test(t *testing.T) {
	setUp()
	var blockList []access.Block
	blockList = append(blockList, 0)
	blocks := access.Blocks{BlockList: blockList}
	testTopic := access.Topic("cars")

	entry := createEntry(100)
	logFileName := blocks.Head().LogFileName(rootPath, testTopic)
	_, _, err := logAccess.Write(logFileName, entry, 0)
	if err != nil {
		t.Error(err)
	}
	_, err = logIndexAccess.WriteFile(logFileName)
	if err != nil {
		t.Error(err)
	}
	index, err := logIndexAccess.Read(logFileName)
	if err != nil {
		t.Error(err)
	}
	offsetBytes, err := readBytesFromByteOffset(afs, string(logFileName), index.Head().ByteOffset, 8)
	if err != nil {
		t.Error(err)
	}
	offsetFromLog := access.Offset(binary.LittleEndian.Uint64(offsetBytes))
	offsetFromIndex := index.Head().Offset
	if offsetFromLog-1 != offsetFromIndex {
		t.Fail()
		t.Logf("expected %d actual %d", offsetFromIndex, offsetFromLog-1)
	}
}

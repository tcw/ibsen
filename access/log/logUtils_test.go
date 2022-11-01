package log

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
	"sync"
	"testing"
)

func TestCreateTopic(t *testing.T) {
	afs := common.MemAfs()
	_, err := CreateTopicDirectory(afs, "tmp", "topic1")
	assert.Nil(t, err)
	exists, err := afs.Exists("tmp/topic1")
	assert.Nil(t, err)
	assert.True(t, exists)
}

func TestListAllFilesInTopic(t *testing.T) {
	afs := common.MemAfs()
	_, err := CreateTopicDirectory(afs, "tmp", "topic1")
	assert.Nil(t, err)
	err = afs.WriteFile("tmp/topic1/001.log", []byte("dummy"), 0600)
	assert.Nil(t, err)
	err = afs.WriteFile("tmp/topic1/001.idx", []byte("dummy"), 0600)
	assert.Nil(t, err)
	err = afs.WriteFile("tmp/topic1/001.dummy", []byte("dummy"), 0600)
	assert.Nil(t, err)
	topics, err := ListAllFilesInTopic(afs, "tmp", "topic1")
	assert.Nil(t, err)
	var files []string
	for _, topic := range topics {
		files = append(files, topic.Name())
	}
	assert.Contains(t, files, "001.log")
	assert.Contains(t, files, "001.idx")
	assert.Contains(t, files, "001.dummy")
}

func TestListAllTopics(t *testing.T) {
	afs := common.MemAfs()
	_, err := CreateTopicDirectory(afs, "tmp", "topic1")
	assert.Nil(t, err)
	_, err = CreateTopicDirectory(afs, "tmp", "topic2")
	assert.Nil(t, err)
	err = afs.MkdirAll("tmp/.git", 0744)
	assert.Nil(t, err)
	topics, err := ListAllTopics(afs, "tmp")
	assert.Nil(t, err)
	assert.Contains(t, topics, "topic1")
	assert.Contains(t, topics, "topic2")
	assert.NotContains(t, topics, ".git")
}

func TestCreateByteEntry(t *testing.T) {
	entry := common.CreateByteEntry([]byte("dummy"), 0)
	afs := common.MemAfs()
	err := afs.WriteFile("tmp/topic1/001.log", entry, 0600)
	assert.Nil(t, err)
	file, err := common.OpenFileForRead(afs, "tmp/topic1/001.log")
	assert.Nil(t, err)
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	go func() {
		_, err = ReadFile(ReadFileParams{
			File:            file,
			LogChan:         logChan,
			Wg:              &wg,
			BatchSize:       10,
			StartByteOffset: 0,
			EndOffset:       100,
		})
		assert.Nil(t, err)
		wg.Done()
	}()
	wg.Wait()
	logEntry := <-logChan
	for _, l := range *logEntry {
		assert.Equal(t, 5, l.ByteSize)
		assert.Equal(t, uint64(0), l.Offset)
		assert.Equal(t, "dummy", string(l.Entry))
	}
}

func TestFindBlockInfo(t *testing.T) {
	afs := common.MemAfs()
	file, err := common.OpenFileForWrite(afs, "tmp/topic1/001.log")
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy1"), 0))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy2"), 1))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy3"), 2))
	assert.Nil(t, err)

	lastOffset, i, err := BlockInfo(afs, "tmp/topic1/001.log")
	assert.Nil(t, err)
	assert.Equal(t, common.Offset(2), lastOffset)
	assert.Equal(t, int64(78), i)
}

func Test(t *testing.T) {
	afs := common.MemAfs()
	fileName := "tmp/topic1/001.log"
	file, err := common.OpenFileForWrite(afs, fileName)
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy1"), 0))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy2"), 1))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy3"), 2))
	assert.Nil(t, err)
	tests := []struct {
		offsetInput        int
		byteOffsetInput    int64
		expectedByteOffset int64
		expectedScanned    int
	}{
		{
			offsetInput:        0,
			byteOffsetInput:    0,
			expectedByteOffset: 0,
			expectedScanned:    0,
		},
		{
			offsetInput:        1,
			byteOffsetInput:    0,
			expectedByteOffset: 26,
			expectedScanned:    1,
		},
		{
			offsetInput:        2,
			byteOffsetInput:    0,
			expectedByteOffset: 52,
			expectedScanned:    2,
		},
		{
			offsetInput:        1,
			byteOffsetInput:    26,
			expectedByteOffset: 26,
			expectedScanned:    0,
		},
		{
			offsetInput:        2,
			byteOffsetInput:    26,
			expectedByteOffset: 52,
			expectedScanned:    1,
		},
		{
			offsetInput:        2,
			byteOffsetInput:    52,
			expectedByteOffset: 52,
			expectedScanned:    0,
		},
		{
			offsetInput:        3,
			byteOffsetInput:    52,
			expectedByteOffset: 78,
			expectedScanned:    1,
		},
		{
			offsetInput:        3,
			byteOffsetInput:    78,
			expectedByteOffset: 78,
			expectedScanned:    0,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("byteOffset %d and offset %d", test.byteOffsetInput, test.offsetInput), func(t *testing.T) {
			byteOffset, scanned, err := FindByteOffsetFromAndIncludingOffset(afs, fileName, test.byteOffsetInput, common.Offset(test.offsetInput))
			assert.Nil(t, err)
			assert.Equal(t, test.expectedByteOffset, byteOffset)
			assert.Equal(t, test.expectedScanned, scanned)
		})
	}
}

func TestLoadTopicBlocks(t *testing.T) {
	afs := common.MemAfs()
	logFileName := "tmp/topic1/001.log"
	indexFileName := "tmp/topic1/001.idx"
	file, err := common.OpenFileForWrite(afs, logFileName)
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(common.CreateByteEntry([]byte("dummy1"), 0))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy2"), 1))
	assert.Nil(t, err)
	_, err = file.Write(common.CreateByteEntry([]byte("dummy3"), 2))
	assert.Nil(t, err)

	idx, _, err := index.CreateBinaryIndexFromLogFile(afs, logFileName, 0, 1)
	assert.Nil(t, err)
	err = afs.WriteFile(indexFileName, idx, 0600)
	assert.Nil(t, err)
	logBlocks, indexBlocks, err := LoadTopicBlocks(afs, "tmp", "topic1")
	assert.Nil(t, err)
	assert.Len(t, logBlocks, 1)
	assert.Len(t, indexBlocks, 1)
}

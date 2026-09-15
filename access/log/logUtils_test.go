package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
	"math"
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
		_, err := ReadFile(ReadFileParams{
			File:            file,
			LogChan:         logChan,
			Wg:              &wg,
			BatchSize:       10,
			StartByteOffset: 0,
			EndOffset:       100,
		})
		assert.Nil(t, err)
	}()
	logEntry := <-logChan
	wg.Done()
	for _, l := range *logEntry {
		assert.Equal(t, binary.LittleEndian.Uint32(entry[:4]), l.Crc)
		assert.Equal(t, 5, l.ByteSize)
		assert.Equal(t, uint64(0), l.Offset)
		assert.Equal(t, "dummy", string(l.Entry))
	}
}

func TestRecoverBlock(t *testing.T) {
	var valid []byte
	for i, payload := range []string{"dummy1", "dummy2", "dummy3"} {
		valid = append(valid, common.CreateByteEntry([]byte(payload), common.Offset(100+i))...)
	}
	withTail := func(tail []byte) []byte {
		return append(append([]byte(nil), valid...), tail...)
	}
	corruptLast := withTail(nil)
	corruptLast[len(corruptLast)-1] ^= 0xff
	tests := []struct {
		name          string
		content       []byte
		wantNext      common.Offset
		wantSize      int64
		wantTruncated int64
	}{
		{name: "clean block", content: valid, wantNext: 103, wantSize: 78},
		{name: "empty block", content: nil, wantNext: 100, wantSize: 0},
		{name: "partial entry", content: withTail(common.CreateByteEntry([]byte("dummy4"), 103)[:15]), wantNext: 103, wantSize: 78, wantTruncated: 15},
		{name: "garbage tail", content: withTail(bytes.Repeat([]byte{0xff}, 40)), wantNext: 103, wantSize: 78, wantTruncated: 40},
		{name: "corrupt last entry", content: corruptLast, wantNext: 102, wantSize: 52, wantTruncated: 26},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			afs := common.MemAfs()
			fileName := "tmp/topic1/100.log"
			assert.Nil(t, afs.WriteFile(fileName, test.content, 0600))
			next, size, truncated, err := RecoverBlock(afs, fileName, 100)
			assert.Nil(t, err)
			assert.Equal(t, test.wantNext, next)
			assert.Equal(t, test.wantSize, size)
			assert.Equal(t, test.wantTruncated, truncated)
			info, err := afs.Stat(fileName)
			assert.Nil(t, err)
			assert.Equal(t, test.wantSize, info.Size())
		})
	}
}

func TestRecoverBlock_offsetGapIsAnError(t *testing.T) {
	afs := common.MemAfs()
	fileName := "tmp/topic1/000.log"
	content := append(common.CreateByteEntry([]byte("dummy1"), 0), common.CreateByteEntry([]byte("dummy2"), 2)...)
	assert.Nil(t, afs.WriteFile(fileName, content, 0600))
	_, _, _, err := RecoverBlock(afs, fileName, 0)
	assert.NotNil(t, err)
	info, err := afs.Stat(fileName)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(content)), info.Size(), "must not truncate valid entries")
}

func collectBatches(params ReadFileParams) ([][]common.LogEntry, error) {
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	params.LogChan = logChan
	params.Wg = &wg
	var batches [][]common.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			batches = append(batches, *batch)
			wg.Done()
		}
		close(done)
	}()
	_, err := ReadFile(params)
	wg.Wait()
	close(logChan)
	<-done
	return batches, err
}

func readFileContent(t *testing.T, content []byte, batchSize uint32) ([][]common.LogEntry, error) {
	afs := common.MemAfs()
	fileName := "tmp/topic1/000.log"
	assert.Nil(t, afs.WriteFile(fileName, content, 0600))
	file, err := common.OpenFileForRead(afs, fileName)
	assert.Nil(t, err)
	defer file.Close()
	return collectBatches(ReadFileParams{File: file, BatchSize: batchSize, EndOffset: math.MaxUint64})
}

func TestReadFile_splitsBatchesAtByteLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1024*1024)
	var content []byte
	for i := 0; i < 25; i++ {
		content = append(content, common.CreateByteEntry(payload, common.Offset(i))...)
	}
	batches, err := readFileContent(t, content, 1000)
	assert.Nil(t, err)
	var sizes []int
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	assert.Equal(t, []int{11, 11, 3}, sizes)
}

func TestReadFile_rejectsCorruptEntry(t *testing.T) {
	content := append(common.CreateByteEntry([]byte("dummy1"), 0), common.CreateByteEntry([]byte("dummy2"), 1)...)
	content[26+12] ^= 0xff // first payload byte of the second entry
	_, err := readFileContent(t, content, 10)
	assert.True(t, errors.Is(err, common.ErrCorruptEntry), "err=%v", err)
}

func TestReadFile_rejectsZeroBatchSize(t *testing.T) {
	_, err := readFileContent(t, common.CreateByteEntry([]byte("dummy1"), 0), 0)
	assert.NotNil(t, err)
}

func TestReadFile_hugeBatchSize(t *testing.T) {
	var content []byte
	for i := 0; i < 3; i++ {
		content = append(content, common.CreateByteEntry([]byte("dummy"), common.Offset(i))...)
	}
	batches, err := readFileContent(t, content, math.MaxUint32)
	assert.Nil(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 3)
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
	logFileName := "tmp/topic1/00000000000000000000.log"
	indexFileName := "tmp/topic1/00000000000000000000.idx"
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

func TestReadFile_stopsWhenCancelled(t *testing.T) {
	var content []byte
	for i := 0; i < 100; i++ {
		content = append(content, common.CreateByteEntry([]byte("dummy"), common.Offset(i))...)
	}
	afs := common.MemAfs()
	fileName := "tmp/topic1/000.log"
	assert.Nil(t, afs.WriteFile(fileName, content, 0600))
	file, err := common.OpenFileForRead(afs, fileName)
	assert.Nil(t, err)
	defer file.Close()

	logChan := make(chan *[]common.LogEntry)
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		// the consumer takes one batch and goes away
		<-logChan
		wg.Done()
		close(cancel)
	}()
	_, err = ReadFile(ReadFileParams{File: file, LogChan: logChan, Wg: &wg, Cancel: cancel, BatchSize: 1, EndOffset: math.MaxUint64})
	assert.True(t, errors.Is(err, common.ErrReadCancelled), "err=%v", err)
	wg.Wait()
}

func TestLoadTopicBlocks_ignoresUnexpectedFiles(t *testing.T) {
	afs := common.MemAfs()
	for _, name := range []string{
		"00000000000000000000.log", "00000000000000000000.idx", "00000000000000000042.log",
		".DS_Store", "README", "notes.txt", "123.log", "1.2.log", "00000000000000000042.log.swp", "99999999999999999999.log",
	} {
		assert.Nil(t, afs.WriteFile("tmp/topic1/"+name, []byte("x"), 0600))
	}
	assert.Nil(t, afs.MkdirAll("tmp/topic1/backup", 0744))
	logBlocks, indexBlocks, err := LoadTopicBlocks(afs, "tmp", "topic1")
	assert.Nil(t, err)
	assert.Equal(t, []common.LogBlock{0, 42}, logBlocks)
	assert.Equal(t, []common.IndexBlock{0}, indexBlocks)
}

func TestListAllTopics_ignoresFiles(t *testing.T) {
	afs := common.MemAfs()
	_, err := CreateTopicDirectory(afs, "tmp", "topic1")
	assert.Nil(t, err)
	assert.Nil(t, afs.WriteFile("tmp/notes.txt", []byte("x"), 0600))
	topics, err := ListAllTopics(afs, "tmp")
	assert.Nil(t, err)
	assert.Equal(t, []string{"topic1"}, topics)
}

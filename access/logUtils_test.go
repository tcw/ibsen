package access

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func memAfs() *afero.Afero {
	var fs = afero.NewMemMapFs()
	return &afero.Afero{Fs: fs}
}

func TestCreateTopic(t *testing.T) {
	afs := memAfs()
	err := CreateTopic(afs, "tmp", "topic1")
	if err != nil {
		t.Error(err)
	}
	exists, err := afs.Exists("tmp/topic1")
	if err != nil {
		t.Error(err)
	}
	assert.True(t, exists)
}

func TestListAllFilesInTopic(t *testing.T) {
	afs := memAfs()
	err := CreateTopic(afs, "tmp", "topic1")
	if err != nil {
		t.Error(err)
	}
	err = afs.WriteFile("tmp/topic1/001.log", []byte("dummy"), 0600)
	if err != nil {
		t.Error(err)
	}
	err = afs.WriteFile("tmp/topic1/001.idx", []byte("dummy"), 0600)
	if err != nil {
		t.Error(err)
	}
	err = afs.WriteFile("tmp/topic1/001.dummy", []byte("dummy"), 0600)
	if err != nil {
		t.Error(err)
	}
	topics, err := ListAllFilesInTopic(afs, "tmp", "topic1")
	if err != nil {
		t.Error(err)
	}
	var files []string
	for _, topic := range topics {
		files = append(files, topic.Name())
	}
	assert.Contains(t, files, "001.log")
	assert.Contains(t, files, "001.idx")
	assert.Contains(t, files, "001.dummy")
}

func TestListAllTopics(t *testing.T) {
	afs := memAfs()
	err := CreateTopic(afs, "tmp", "topic1")
	if err != nil {
		t.Error(err)
	}
	err = CreateTopic(afs, "tmp", "topic2")
	if err != nil {
		t.Error(err)
	}
	err = afs.MkdirAll("tmp/.git", 0744)
	if err != nil {
		t.Error(err)
	}
	topics, err := ListAllTopics(afs, "tmp")
	if err != nil {
		t.Error(err)
	}
	assert.Contains(t, topics, "topic1")
	assert.Contains(t, topics, "topic2")
	assert.NotContains(t, topics, ".git")
}

func TestCreateByteEntry(t *testing.T) {
	entry := CreateByteEntry([]byte("dummy"), 0)
	afs := memAfs()
	err := afs.WriteFile("tmp/topic1/001.log", entry, 0600)
	if err != nil {
		t.Error(err)
	}
	file, err := OpenFileForRead(afs, "tmp/topic1/001.log")
	if err != nil {
		t.Error(err)
	}
	logChan := make(chan *[]LogEntry)
	var wg sync.WaitGroup
	go func() {
		err = ReadFile(file, logChan, &wg, 10, 0, 100)
		if err != nil {
			t.Error(err)
		}
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
	afs := memAfs()
	file, err := openFileForWrite(afs, "tmp/topic1/001.log")
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy1"), 0))
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy2"), 1))
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy3"), 2))
	if err != nil {
		t.Error(err)
	}

	lastOffset, i, err := FindBlockInfo(afs, "tmp/topic1/001.log")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, Offset(2), lastOffset)
	assert.Equal(t, int64(78), i)
}

func Test(t *testing.T) {
	afs := memAfs()
	fileName := "tmp/topic1/001.log"
	file, err := openFileForWrite(afs, fileName)
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy1"), 0))
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy2"), 1))
	if err != nil {
		t.Error(err)
	}
	_, err = file.Write(CreateByteEntry([]byte("dummy3"), 2))
	if err != nil {
		t.Error(err)
	}
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
			byteOffset, scanned, err := FindByteOffsetFromAndIncludingOffset(afs, fileName, test.byteOffsetInput, Offset(test.offsetInput))
			assert.Nil(t, err)
			assert.Equal(t, test.expectedByteOffset, byteOffset)
			assert.Equal(t, test.expectedScanned, scanned)
		})
	}
}

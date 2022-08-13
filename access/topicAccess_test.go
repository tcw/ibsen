package access

import (
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

func TestTopic_Write(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 1024 * 1024,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	lastOffset, _, err := BlockInfo(afs, "tmp/topic1/00000000000000000000.log")
	assert.Nil(t, err)
	assert.Equal(t, Offset(9), lastOffset)
}

func TestTopic_Load(t *testing.T) {
	afs := memAfs()
	//topic := NewLogTopic(afs, "tmp", "topic1", 2000, false)
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	assert.Equal(t, Offset(10), topic.NextOffset)
	assert.Len(t, topic.LogBlockList, 1)
}

func TestTopic_Read_one_batch(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	logChan := make(chan *[]LogEntry)
	var wg sync.WaitGroup
	go func() {
		_, err := topic.ReadLog(ReadLogParams{
			LogChan:   logChan,
			Wg:        &wg,
			From:      0,
			BatchSize: 100,
		})
		assert.Nil(t, err)
		wg.Done()
	}()
	wg.Wait()
	logEntry := <-logChan
	for i, l := range *logEntry {
		assert.Equal(t, uint64(i), l.Offset)
		assert.Equal(t, "dummy"+strconv.Itoa(i), string(l.Entry))
	}
}

func TestTopic_Read_multiple_batches(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	logChan := make(chan *[]LogEntry)
	var wg sync.WaitGroup
	go func() {
		_, err := topic.ReadLog(ReadLogParams{
			LogChan:   logChan,
			Wg:        &wg,
			From:      0,
			BatchSize: 100,
		})
		assert.Nil(t, err)
		wg.Done()
	}()
	wg.Wait()
	logEntry := <-logChan
	for i, l := range *logEntry {
		assert.Equal(t, uint64(i), l.Offset)
		assert.Equal(t, "dummy"+strconv.Itoa(i), string(l.Entry))
	}
}

func TestTopic_UpdateIndex_sigle_block(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 20000,
	})
	err := topic.Write(createInputEntries(100))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	err = topic.Write(createInputEntries(100))
	assert.Nil(t, err)
	topic.indexWg.Wait()
	updatedIndex, err := topic.UpdateIndex()
	assert.Nil(t, err)
	assert.True(t, updatedIndex)
	head, hasHead := topic.indexBlockHead()
	topic.indexBlockHead()
	assert.True(t, hasHead)
	index, err := topic.getIndexFromIndexBlock(head)
	assert.Nil(t, err)
	assert.Equal(t, Offset(190), index.Head().Offset)
}

func TestTopic_UpdateIndex_multiple_blocks(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	err = topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	topic.indexWg.Wait()
	updatedIndex, err := topic.UpdateIndex()
	assert.Nil(t, err)
	assert.True(t, updatedIndex)
	head, hasHead := topic.indexBlockHead()
	topic.indexBlockHead()
	assert.True(t, hasHead)
	index, err := topic.getIndexFromIndexBlock(head)
	assert.Nil(t, err)
	assert.Equal(t, Offset(2990), index.Head().Offset)
}

func createInputEntries(numberOfEntries int) *[][]byte {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, []byte("dummy"+strconv.Itoa(i)))
	}
	return &tmpBytes
}

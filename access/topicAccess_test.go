package access

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

func TestTopic_Write(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 1024*1024, true)
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	lastOffset, _, err := BlockInfo(afs, "tmp/topic1/00000000000000000000.log")
	assert.Nil(t, err)
	assert.Equal(t, Offset(9), lastOffset)
}

func TestTopic_Load(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 2000, false)
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	assert.Equal(t, Offset(10), topic.NextOffset)
	assert.Len(t, topic.LogBlockList, 1)
}

func TestTopic_Read_one_batch(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 2000, false)
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	logChan := make(chan *[]LogEntry)
	var wg sync.WaitGroup
	go func() {
		err := topic.Read(logChan, &wg, 0, 100)
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

func TestTopic_UpdateIndex(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 20000, false)
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
	fmt.Println("Done")
}

func createInputEntries(numberOfEntries int) *[][]byte {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, []byte("dummy"+strconv.Itoa(i)))
	}
	return &tmpBytes
}

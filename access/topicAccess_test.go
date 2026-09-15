package access

import (
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access/common"
	ibsLog "github.com/tcw/ibsen/access/log"
	"strconv"
	"testing"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

func TestTopic_Write(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 1024 * 1024,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	nextOffset, _, truncated, err := ibsLog.RecoverBlock(afs, "tmp/topic1/00000000000000000000.log", 0)
	assert.Nil(t, err)
	assert.Equal(t, common.Offset(10), nextOffset)
	assert.Equal(t, int64(0), truncated)
}

func TestTopic_Load(t *testing.T) {
	afs := common.MemAfs()
	//topic := NewLogTopic(afs, "tmp", "topic1", 2000, false)
	topic := NewLogTopic(common.TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.LoadOrCreate()
	assert.Nil(t, err)
	assert.Equal(t, common.Offset(10), topic.NextOffset)
	assert.Len(t, topic.LogBlockList, 1)
}

func TestTopic_Read_one_batch(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.LoadOrCreate()
	assert.Nil(t, err)
	entries, err := readAllFrom(topic, 0, 100)
	assert.Nil(t, err)
	assert.Len(t, entries, 10)
	for i, l := range entries {
		assert.Equal(t, uint64(i), l.Offset)
		assert.Equal(t, "dummy"+strconv.Itoa(i), string(l.Entry))
	}
}

func TestTopic_Read_multiple_batches(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{
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
	err = topic.LoadOrCreate()
	assert.Nil(t, err)
	entries, err := readAllFrom(topic, 0, 100)
	assert.Nil(t, err)
	assert.Len(t, entries, 3000)
	for i, l := range entries {
		assert.Equal(t, uint64(i), l.Offset)
		assert.Equal(t, "dummy"+strconv.Itoa(i%1000), string(l.Entry))
	}
}

func TestTopic_UpdateIndex_sigle_block(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 20000,
	})
	err := topic.Write(createInputEntries(100))
	assert.Nil(t, err)
	err = topic.LoadOrCreate()
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
	assert.Equal(t, common.Offset(190), index.Head().Offset)
}

func TestTopic_UpdateIndex_multiple_blocks(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{
		Afs:          afs,
		RootPath:     "tmp",
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(1000))
	assert.Nil(t, err)
	err = topic.LoadOrCreate()
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
	assert.Equal(t, common.Offset(2990), index.Head().Offset)
}

func createInputEntries(numberOfEntries int) *[][]byte {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, []byte("dummy"+strconv.Itoa(i)))
	}
	return &tmpBytes
}

func TestTopic_rejectsInvalidTopicName(t *testing.T) {
	afs := common.MemAfs()
	topic := NewLogTopic(common.TopicParams{Afs: afs, RootPath: "tmp", TopicName: "../escaped", MaxBlockSize: 1000})
	assert.ErrorIs(t, topic.LoadOrCreate(), common.ErrInvalidTopicName)
	assert.ErrorIs(t, topic.Write(createInputEntries(3)), common.ErrInvalidTopicName)
	assert.ErrorIs(t, topic.Read(common.ReadLogParams{BatchSize: 10}), common.ErrInvalidTopicName)
	exists, err := afs.Exists("escaped")
	assert.Nil(t, err)
	assert.False(t, exists)
}

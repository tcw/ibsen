package topic

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
)

func TestTopic_Write(t *testing.T) {
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
		TopicName:    "topic1",
		MaxBlockSize: 1024 * 1024,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	blocks, err := store.List("topic1", driven.Log)
	assert.Nil(t, err)
	assert.Len(t, blocks, 1)
	nextOffset, _, truncated, err := logfmt.RecoverBlock(store, driven.LogRef("topic1", 0), 0, blocks[0].Size)
	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(10), nextOffset)
	assert.Equal(t, int64(0), truncated)
}

func TestTopic_Load(t *testing.T) {
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	err := topic.Write(createInputEntries(10))
	assert.Nil(t, err)
	err = topic.LoadOrCreate()
	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(10), topic.NextOffset)
	assert.Len(t, topic.LogBlockList, 1)
}

func TestTopic_Read_one_batch(t *testing.T) {
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
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
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
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
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
		TopicName:    "topic1",
		MaxBlockSize: 20000,
	})
	writeOneByOne(t, topic, 0, 100)
	err := topic.LoadOrCreate()
	assert.Nil(t, err)
	writeOneByOne(t, topic, 100, 100)
	topic.indexWg.Wait()
	updatedIndex, err := topic.UpdateIndex()
	assert.Nil(t, err)
	assert.True(t, updatedIndex)
	head, hasHead := topic.indexBlockHead()
	topic.indexBlockHead()
	assert.True(t, hasHead)
	index, err := topic.getIndexFromIndexBlock(head)
	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(190), index.Head().Offset)
}

func TestTopic_UpdateIndex_multiple_blocks(t *testing.T) {
	store, _ := newTestStore(t)
	topic := NewLogTopic(Params{
		Store:        store,
		TopicName:    "topic1",
		MaxBlockSize: 2000,
	})
	writeOneByOne(t, topic, 0, 1000)
	err := topic.LoadOrCreate()
	assert.Nil(t, err)
	writeOneByOne(t, topic, 1000, 1000)
	writeOneByOne(t, topic, 2000, 1000)
	topic.indexWg.Wait()
	updatedIndex, err := topic.UpdateIndex()
	assert.Nil(t, err)
	assert.True(t, updatedIndex)
	head, hasHead := topic.indexBlockHead()
	topic.indexBlockHead()
	assert.True(t, hasHead)
	index, err := topic.getIndexFromIndexBlock(head)
	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(2990), index.Head().Offset)
}

// writeOneByOne writes count entries from an offset, one to a write and so one to a frame.
// An index pair points at a frame start, so a test about which offsets end up indexed has to
// say how the entries are framed.
func writeOneByOne(t *testing.T, topic *Topic, from, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		entries := [][]byte{[]byte("dummy" + strconv.Itoa(from+i))}
		if err := topic.Write(&entries); err != nil {
			t.Fatal(err)
		}
	}
}

func createInputEntries(numberOfEntries int) *[][]byte {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, []byte("dummy"+strconv.Itoa(i)))
	}
	return &tmpBytes
}

func TestTopic_rejectsInvalidTopicName(t *testing.T) {
	store, afs := newTestStore(t)
	topic := NewLogTopic(Params{Store: store, TopicName: "../escaped", MaxBlockSize: 1000})
	assert.ErrorIs(t, topic.LoadOrCreate(), domain.ErrInvalidTopicName)
	assert.ErrorIs(t, topic.Write(createInputEntries(3)), domain.ErrInvalidTopicName)
	assert.ErrorIs(t, topic.Read(domain.ReadLogParams{BatchSize: 10}), domain.ErrInvalidTopicName)
	exists, err := afs.Exists("escaped")
	assert.Nil(t, err)
	assert.False(t, exists)
	topics, err := store.Topics()
	assert.Nil(t, err)
	assert.Empty(t, topics)
}

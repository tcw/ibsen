package access

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestTopic_Write(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 1024*1024, true)
	err := topic.Write(createInputEntries(10, 100))
	assert.Nil(t, err)
	lastOffset, _, err := BlockInfo(afs, "tmp/topic1/00000000000000000000.log")
	assert.Nil(t, err)
	assert.Equal(t, Offset(9), lastOffset)
}

func TestTopic_Load(t *testing.T) {
	afs := memAfs()
	topic := NewLogTopic(afs, "tmp", "topic1", 2000, false)
	err := topic.Write(createInputEntries(10, 100))
	assert.Nil(t, err)
	err = topic.Load()
	assert.Nil(t, err)
	assert.Equal(t, Offset(10), topic.NextOffset)
	assert.Len(t, topic.LogBlockList, 1)
}

func TestTopic_Read(t *testing.T) {

}

func createInputEntries(numberOfEntries int, entryByteSize int) *[][]byte {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createTestValues(entryByteSize))
	}
	return &tmpBytes
}

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzæøåABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ1234567890")
	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

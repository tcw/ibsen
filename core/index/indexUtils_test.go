package index

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/core/domain"
)

func TestCreateIndex(t *testing.T) {
	indexBytes, _, err := CreateBinaryIndexFromLog(bytes.NewReader(createLogEntries(10)), 0, 1)
	assert.Nil(t, err)
	index := NewIndex(indexBytes)
	// every entry is indexed when oneEntryForEvery is 1, including the block's first
	assert.Equal(t, 10, index.Size())
}

func createLogEntries(entries int) []byte {
	var log = make([]byte, 0)
	for i := 0; i < entries; i++ {
		log = append(log, domain.CreateByteEntry([]byte("dummy"+strconv.Itoa(i)), domain.Offset(i))...)
	}
	return log
}

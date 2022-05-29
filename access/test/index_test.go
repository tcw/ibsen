package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access"
	"testing"
)

func TestFindNearestOffset(t *testing.T) {
	index := createIndex(10, 10)
	tests := []struct {
		name               string
		input              uint64
		expectedOffset     uint64
		expectedByteOffset int64
	}{
		{
			name:               "before existing index offsets",
			input:              3,
			expectedOffset:     0,
			expectedByteOffset: 0,
		},
		{
			name:               "after existing index offsets",
			input:              1999,
			expectedOffset:     90,
			expectedByteOffset: 900,
		},
		{
			name:               "in range of existing index offsets",
			input:              45,
			expectedOffset:     40,
			expectedByteOffset: 400,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			offset := index.FindNearestByteOffset(access.Offset(test.input))
			assert.Equal(t, access.Offset(test.expectedOffset), offset.Offset)
			assert.Equal(t, test.expectedByteOffset, offset.ByteOffset)
		})
	}
}

func TestHeadEmpty(t *testing.T) {
	index := createIndex(0, 0)
	head := index.Head()
	assert.Equal(t, access.Offset(0), head.Offset)
	assert.Equal(t, int64(0), head.ByteOffset)
}

func TestHead(t *testing.T) {
	index := createIndex(10, 10)
	head := index.Head()
	assert.Equal(t, access.Offset(90), head.Offset)
	assert.Equal(t, int64(900), head.ByteOffset)
}

// creates series offset [10,20,30,...] byteOffset [100,200,300,...]
func createIndex(every int, entries int) access.Index {
	var indexOffests []access.IndexOffset
	for i := 0; i < entries; i++ {
		indexOffset := access.IndexOffset{
			Offset:     access.Offset(i * every),
			ByteOffset: int64(i * every * 10),
		}
		indexOffests = append(indexOffests, indexOffset)
	}
	return access.Index{IndexOffsets: indexOffests}
}

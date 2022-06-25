package access

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindNearestOffset(t *testing.T) {
	index := createTestIndex(10, 10)
	tests := []struct {
		name               string
		inputOffset        uint64
		expectedOffset     uint64
		expectedByteOffset int64
	}{
		{
			name:               "before existing index offsets",
			inputOffset:        3,
			expectedOffset:     0,
			expectedByteOffset: 0,
		},
		{
			name:               "after existing index offsets",
			inputOffset:        1999,
			expectedOffset:     90,
			expectedByteOffset: 900,
		},
		{
			name:               "in range of existing index offsets",
			inputOffset:        45,
			expectedOffset:     40,
			expectedByteOffset: 400,
		},
		{
			name:               "on offset",
			inputOffset:        40,
			expectedOffset:     40,
			expectedByteOffset: 400,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			offset := index.findNearestByteOffset(Offset(test.inputOffset))
			assert.Equal(t, Offset(test.expectedOffset), offset.Offset)
			assert.Equal(t, test.expectedByteOffset, offset.ByteOffset)
		})
	}
}

func TestHeadEmpty(t *testing.T) {
	index := createTestIndex(0, 0)
	head := index.Head()
	assert.Equal(t, Offset(0), head.Offset)
	assert.Equal(t, int64(0), head.ByteOffset)
}

func TestHead(t *testing.T) {
	index := createTestIndex(10, 10)
	head := index.Head()
	assert.Equal(t, Offset(90), head.Offset)
	assert.Equal(t, int64(900), head.ByteOffset)
}

// creates series offset [10,20,30,...] byteOffset [100,200,300,...]
func createTestIndex(every int, entries int) Index {
	var indexOffests []IndexOffset
	for i := 0; i < entries; i++ {
		indexOffset := IndexOffset{
			Offset:     Offset(i * every),
			ByteOffset: int64(i * every * 10),
		}
		indexOffests = append(indexOffests, indexOffset)
	}
	return Index{IndexOffsets: indexOffests}
}

package index

import (
	"bytes"
	"errors"
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

// A sparsity of zero would reach "offset % 0" and panic, so it is refused at the boundary
// rather than left to the modulo.
func TestCreateBinaryIndexFromLogRefusesZeroSparsity(t *testing.T) {
	log := bytes.NewReader(domain.CreateByteEntry([]byte("one"), domain.Offset(0)))

	pairs, byteOffset, err := CreateBinaryIndexFromLog(log, 0, 0)

	if !errors.Is(err, ErrInvalidSparsity) {
		t.Fatalf("got %v, want ErrInvalidSparsity", err)
	}
	if pairs != nil {
		t.Errorf("got %v pairs alongside the error, want none", pairs)
	}
	if byteOffset != 0 {
		t.Errorf("byte offset moved to %d on a refused call", byteOffset)
	}
}

// Sparsity 1 indexes every entry, which is the densest an index gets.
func TestSparsityOneIndexesEveryEntry(t *testing.T) {
	var log bytes.Buffer
	const entries = 5
	for i := 0; i < entries; i++ {
		log.Write(domain.CreateByteEntry([]byte("e"), domain.Offset(i)))
	}

	pairs, _, err := CreateBinaryIndexFromLog(bytes.NewReader(log.Bytes()), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(pairs) / PairSize; got != entries {
		t.Errorf("indexed %d of %d entries at sparsity 1", got, entries)
	}
}

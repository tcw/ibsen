package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access/blockstore/aferostore"
	"github.com/tcw/ibsen/access/common"
)

// blockWith puts content in a log block and returns the store holding it, its reference and
// the size the store reports. The afero adapter stands in for any BlockStore here.
func blockWith(t *testing.T, block common.LogBlock, content []byte) (common.BlockStore, common.BlockRef, int64) {
	t.Helper()
	store, _ := aferostore.NewMem("tmp")
	ref := common.LogRef("topic1", block)
	if _, err := store.Append(ref, content); err != nil {
		t.Fatal(err)
	}
	blocks, err := store.List("topic1", common.Log)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("list: %v, %v", blocks, err)
	}
	return store, ref, blocks[0].Size
}

func TestCreateByteEntry(t *testing.T) {
	entry := common.CreateByteEntry([]byte("dummy"), 0)
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	go func() {
		_, err := ReadFile(ReadFileParams{
			Reader:    bytes.NewReader(entry),
			LogChan:   logChan,
			Wg:        &wg,
			BatchSize: 10,
			EndOffset: 100,
		})
		assert.Nil(t, err)
	}()
	logEntry := <-logChan
	wg.Done()
	for _, l := range *logEntry {
		assert.Equal(t, binary.LittleEndian.Uint32(entry[:4]), l.Crc)
		assert.Equal(t, 5, l.ByteSize)
		assert.Equal(t, uint64(0), l.Offset)
		assert.Equal(t, "dummy", string(l.Entry))
	}
}

func TestRecoverBlock(t *testing.T) {
	var valid []byte
	for i, payload := range []string{"dummy1", "dummy2", "dummy3"} {
		valid = append(valid, common.CreateByteEntry([]byte(payload), common.Offset(100+i))...)
	}
	withTail := func(tail []byte) []byte {
		return append(append([]byte(nil), valid...), tail...)
	}
	corruptLast := withTail(nil)
	corruptLast[len(corruptLast)-1] ^= 0xff
	tests := []struct {
		name          string
		content       []byte
		wantNext      common.Offset
		wantSize      int64
		wantTruncated int64
	}{
		{name: "clean block", content: valid, wantNext: 103, wantSize: 78},
		{name: "empty block", content: nil, wantNext: 100, wantSize: 0},
		{name: "partial entry", content: withTail(common.CreateByteEntry([]byte("dummy4"), 103)[:15]), wantNext: 103, wantSize: 78, wantTruncated: 15},
		{name: "garbage tail", content: withTail(bytes.Repeat([]byte{0xff}, 40)), wantNext: 103, wantSize: 78, wantTruncated: 40},
		{name: "corrupt last entry", content: corruptLast, wantNext: 102, wantSize: 52, wantTruncated: 26},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, ref, size := blockWith(t, 100, test.content)
			next, validSize, truncated, err := RecoverBlock(store, ref, 100, size)
			assert.Nil(t, err)
			assert.Equal(t, test.wantNext, next)
			assert.Equal(t, test.wantSize, validSize)
			assert.Equal(t, test.wantTruncated, truncated)
			blocks, err := store.List("topic1", common.Log)
			assert.Nil(t, err)
			assert.Equal(t, test.wantSize, blocks[0].Size)
		})
	}
}

func TestRecoverBlock_offsetGapIsAnError(t *testing.T) {
	content := append(common.CreateByteEntry([]byte("dummy1"), 0), common.CreateByteEntry([]byte("dummy2"), 2)...)
	store, ref, size := blockWith(t, 0, content)
	_, _, _, err := RecoverBlock(store, ref, 0, size)
	assert.NotNil(t, err)
	blocks, err := store.List("topic1", common.Log)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(content)), blocks[0].Size, "must not truncate valid entries")
}

func collectBatches(params ReadFileParams) ([][]common.LogEntry, error) {
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	params.LogChan = logChan
	params.Wg = &wg
	var batches [][]common.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			batches = append(batches, *batch)
			wg.Done()
		}
		close(done)
	}()
	_, err := ReadFile(params)
	wg.Wait()
	close(logChan)
	<-done
	return batches, err
}

func readFileContent(t *testing.T, content []byte, batchSize uint32) ([][]common.LogEntry, error) {
	t.Helper()
	return collectBatches(ReadFileParams{Reader: bytes.NewReader(content), BatchSize: batchSize, EndOffset: math.MaxUint64})
}

func TestReadFile_splitsBatchesAtByteLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1024*1024)
	var content []byte
	for i := 0; i < 25; i++ {
		content = append(content, common.CreateByteEntry(payload, common.Offset(i))...)
	}
	batches, err := readFileContent(t, content, 1000)
	assert.Nil(t, err)
	var sizes []int
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	assert.Equal(t, []int{11, 11, 3}, sizes)
}

func TestReadFile_rejectsCorruptEntry(t *testing.T) {
	content := append(common.CreateByteEntry([]byte("dummy1"), 0), common.CreateByteEntry([]byte("dummy2"), 1)...)
	content[26+12] ^= 0xff // first payload byte of the second entry
	_, err := readFileContent(t, content, 10)
	assert.True(t, errors.Is(err, common.ErrCorruptEntry), "err=%v", err)
}

func TestReadFile_rejectsZeroBatchSize(t *testing.T) {
	_, err := readFileContent(t, common.CreateByteEntry([]byte("dummy1"), 0), 0)
	assert.NotNil(t, err)
}

func TestReadFile_hugeBatchSize(t *testing.T) {
	var content []byte
	for i := 0; i < 3; i++ {
		content = append(content, common.CreateByteEntry([]byte("dummy"), common.Offset(i))...)
	}
	batches, err := readFileContent(t, content, math.MaxUint32)
	assert.Nil(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 3)
}

// TestReadFile_rejectsUnexpectedFirstOffset guards the assertion that replaced the look back
// a seekable file allowed: the caller says which offset the reader starts at.
func TestReadFile_rejectsUnexpectedFirstOffset(t *testing.T) {
	content := common.CreateByteEntry([]byte("dummy1"), 7)
	_, err := collectBatches(ReadFileParams{
		Reader:     bytes.NewReader(content),
		BatchSize:  10,
		FromOffset: 5,
		EndOffset:  math.MaxUint64,
	})
	assert.NotNil(t, err)
}

func TestFindByteOffsetFromAndIncludingOffset(t *testing.T) {
	var content []byte
	for i, payload := range []string{"dummy1", "dummy2", "dummy3"} {
		content = append(content, common.CreateByteEntry([]byte(payload), common.Offset(i))...)
	}
	store, ref, _ := blockWith(t, 0, content)
	tests := []struct {
		offsetInput        int
		byteOffsetInput    int64
		expectedByteOffset int64
		expectedScanned    int
	}{
		{offsetInput: 0, byteOffsetInput: 0, expectedByteOffset: 0, expectedScanned: 0},
		{offsetInput: 1, byteOffsetInput: 0, expectedByteOffset: 26, expectedScanned: 1},
		{offsetInput: 2, byteOffsetInput: 0, expectedByteOffset: 52, expectedScanned: 2},
		{offsetInput: 1, byteOffsetInput: 26, expectedByteOffset: 26, expectedScanned: 0},
		{offsetInput: 2, byteOffsetInput: 26, expectedByteOffset: 52, expectedScanned: 1},
		{offsetInput: 2, byteOffsetInput: 52, expectedByteOffset: 52, expectedScanned: 0},
		{offsetInput: 3, byteOffsetInput: 52, expectedByteOffset: 78, expectedScanned: 1},
		{offsetInput: 3, byteOffsetInput: 78, expectedByteOffset: 78, expectedScanned: 0},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("byteOffset %d and offset %d", test.byteOffsetInput, test.offsetInput), func(t *testing.T) {
			byteOffset, scanned, err := FindByteOffsetFromAndIncludingOffset(store, ref, test.byteOffsetInput, common.Offset(test.offsetInput))
			assert.Nil(t, err)
			assert.Equal(t, test.expectedByteOffset, byteOffset)
			assert.Equal(t, test.expectedScanned, scanned)
		})
	}
}

func TestReadEntryAt(t *testing.T) {
	var content []byte
	for i, payload := range []string{"dummy1", "dummy2", "dummy3"} {
		content = append(content, common.CreateByteEntry([]byte(payload), common.Offset(i))...)
	}
	store, ref, _ := blockWith(t, 0, content)
	entry, n, err := ReadEntryAt(store, ref, 26)
	assert.Nil(t, err)
	assert.Equal(t, 26, n)
	assert.Equal(t, uint64(1), entry.Offset)
	assert.Equal(t, "dummy2", string(entry.Entry))
}

func TestReadFile_stopsWhenCancelled(t *testing.T) {
	var content []byte
	for i := 0; i < 100; i++ {
		content = append(content, common.CreateByteEntry([]byte("dummy"), common.Offset(i))...)
	}
	logChan := make(chan *[]common.LogEntry)
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		// the consumer takes one batch and goes away
		<-logChan
		wg.Done()
		close(cancel)
	}()
	_, err := ReadFile(ReadFileParams{Reader: bytes.NewReader(content), LogChan: logChan, Wg: &wg, Cancel: cancel, BatchSize: 1, EndOffset: math.MaxUint64})
	assert.True(t, errors.Is(err, common.ErrReadCancelled), "err=%v", err)
	wg.Wait()
}

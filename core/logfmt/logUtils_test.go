package logfmt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/adapter/driven/blockstore/aferostore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// blockWith puts content in a log block and returns the store holding it, its reference and
// the size the store reports. The afero adapter stands in for any BlockStore here.
func blockWith(t *testing.T, block domain.LogBlock, content []byte) (driven.BlockStore, driven.BlockRef, int64) {
	t.Helper()
	store, _ := aferostore.NewMem("tmp")
	ref := driven.LogRef("topic1", block)
	if _, err := store.Append(ref, content); err != nil {
		t.Fatal(err)
	}
	blocks, err := store.List("topic1", driven.Log)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("list: %v, %v", blocks, err)
	}
	return store, ref, blocks[0].Size
}

// entriesFrom encodes payloads as entries numbered from firstOffset.
func entriesFrom(firstOffset domain.Offset, payloads ...string) []byte {
	var entries []byte
	for i, payload := range payloads {
		entries = append(entries, domain.CreateByteEntry([]byte(payload), firstOffset+domain.Offset(i))...)
	}
	return entries
}

// oneFrame builds a single frame holding payloads numbered from firstOffset.
func oneFrame(t *testing.T, firstOffset domain.Offset, payloads ...string) []byte {
	t.Helper()
	frame, err := EncodeFrame(driven.NoCodec{}, firstOffset, len(payloads), entriesFrom(firstOffset, payloads...))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// framePerEntry builds a block giving every payload a frame of its own, which is what a
// stream of single-entry writes produces.
func framePerEntry(t *testing.T, firstOffset domain.Offset, payloads ...string) []byte {
	t.Helper()
	var block []byte
	for i, payload := range payloads {
		block = append(block, oneFrame(t, firstOffset+domain.Offset(i), payload)...)
	}
	return block
}

func TestCreateByteEntry(t *testing.T) {
	entry := domain.CreateByteEntry([]byte("dummy"), 0)
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	go func() {
		_, err := ReadFile(ReadFileParams{
			Reader:    bytes.NewReader(oneFrame(t, 0, "dummy")),
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
	valid := framePerEntry(t, 100, "dummy1", "dummy2", "dummy3")
	frameSize := int64(len(valid) / 3)
	withTail := func(tail []byte) []byte {
		return append(append([]byte(nil), valid...), tail...)
	}
	corruptLast := withTail(nil)
	corruptLast[len(corruptLast)-1] ^= 0xff
	tests := []struct {
		name          string
		content       []byte
		wantNext      domain.Offset
		wantSize      int64
		wantTruncated int64
	}{
		{name: "clean block", content: valid, wantNext: 103, wantSize: 3 * frameSize},
		{name: "empty block", content: nil, wantNext: 100, wantSize: 0},
		{name: "partial header", content: withTail(oneFrame(t, 103, "dummy4")[:15]), wantNext: 103, wantSize: 3 * frameSize, wantTruncated: 15},
		{name: "partial payload", content: withTail(oneFrame(t, 103, "dummy4")[:domain.FrameHeaderSize+4]), wantNext: 103, wantSize: 3 * frameSize, wantTruncated: domain.FrameHeaderSize + 4},
		{name: "garbage tail", content: withTail(bytes.Repeat([]byte{0xff}, 40)), wantNext: 103, wantSize: 3 * frameSize, wantTruncated: 40},
		// a flipped byte in the last frame's payload fails the payload checksum, so the
		// whole frame goes: a frame is the smallest thing that can be kept
		{name: "corrupt last frame", content: corruptLast, wantNext: 102, wantSize: 2 * frameSize, wantTruncated: frameSize},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, ref, size := blockWith(t, 100, test.content)
			next, validSize, truncated, err := RecoverBlock(store, ref, 100, size)
			assert.Nil(t, err)
			assert.Equal(t, test.wantNext, next)
			assert.Equal(t, test.wantSize, validSize)
			assert.Equal(t, test.wantTruncated, truncated)
			blocks, err := store.List("topic1", driven.Log)
			assert.Nil(t, err)
			assert.Equal(t, test.wantSize, blocks[0].Size)
		})
	}
}

func TestRecoverBlock_offsetGapIsAnError(t *testing.T) {
	content := append(oneFrame(t, 0, "dummy1"), oneFrame(t, 2, "dummy2")...)
	store, ref, size := blockWith(t, 0, content)
	_, _, _, err := RecoverBlock(store, ref, 0, size)
	assert.NotNil(t, err)
	blocks, err := store.List("topic1", driven.Log)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(content)), blocks[0].Size, "must not truncate valid entries")
}

// A block written before framing is not damaged, so it is reported rather than truncated
// away. This is the whole of what the clean break looks like to an operator.
func TestRecoverBlock_blockWrittenBeforeFramingIsReported(t *testing.T) {
	content := entriesFrom(0, "dummy1", "dummy2")
	store, ref, size := blockWith(t, 0, content)

	_, _, _, err := RecoverBlock(store, ref, 0, size)

	if !errors.Is(err, domain.ErrUnsupportedLogFormat) {
		t.Fatalf("got %v, want ErrUnsupportedLogFormat", err)
	}
	blocks, err := store.List("topic1", driven.Log)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(content)), blocks[0].Size, "must not truncate a block it does not understand")
}

// Recovery checks frames against their two checksums and decodes nothing, so a torn tail is
// found and cut even by a build carrying none of the codecs the block was written with.
func TestRecoverBlock_needsNoCodec(t *testing.T) {
	unknown, err := EncodeFrame(unknownCodec{}, 0, 1, entriesFrom(0, "dummy1"))
	if err != nil {
		t.Fatal(err)
	}
	content := append(append([]byte(nil), unknown...), bytes.Repeat([]byte{0xff}, 40)...)
	store, ref, size := blockWith(t, 0, content)

	next, validSize, truncated, err := RecoverBlock(store, ref, 0, size)

	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(1), next)
	assert.Equal(t, int64(len(unknown)), validSize)
	assert.Equal(t, int64(40), truncated)
}

// unknownCodec writes frames naming a codec no registry here holds.
type unknownCodec struct{}

func (unknownCodec) ID() driven.CodecID { return driven.CodecID(200) }
func (unknownCodec) Encode(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (unknownCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src...), nil
}

func collectBatches(params ReadFileParams) ([][]domain.LogEntry, error) {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	params.LogChan = logChan
	params.Wg = &wg
	var batches [][]domain.LogEntry
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

func readFileContent(t *testing.T, content []byte, batchSize uint32) ([][]domain.LogEntry, error) {
	t.Helper()
	return collectBatches(ReadFileParams{Reader: bytes.NewReader(content), BatchSize: batchSize, EndOffset: math.MaxUint64})
}

func TestReadFile_splitsBatchesAtByteLimit(t *testing.T) {
	payload := string(bytes.Repeat([]byte("x"), 1024*1024))
	var content []byte
	for i := 0; i < 25; i++ {
		content = append(content, oneFrame(t, domain.Offset(i), payload)...)
	}
	batches, err := readFileContent(t, content, 1000)
	assert.Nil(t, err)
	var sizes []int
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	assert.Equal(t, []int{11, 11, 3}, sizes)
}

// A flipped byte anywhere in a frame's payload fails the frame's own checksum, which is the
// outer of the two a read passes through.
func TestReadFile_rejectsCorruptFrame(t *testing.T) {
	content := append(oneFrame(t, 0, "dummy1"), oneFrame(t, 1, "dummy2")...)
	content[len(content)-1] ^= 0xff
	_, err := readFileContent(t, content, 10)
	assert.True(t, errors.Is(err, domain.ErrCorruptFrame), "err=%v", err)
}

// The entry checksum still earns its place inside a frame: a frame that verifies whole can
// still hold an entry that does not, and the read says so rather than handing it over.
func TestReadFile_rejectsCorruptEntryInsideAValidFrame(t *testing.T) {
	entries := entriesFrom(0, "dummy1", "dummy2")
	entries[26+12] ^= 0xff // first payload byte of the second entry
	content, err := EncodeFrame(driven.NoCodec{}, 0, 2, entries)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readFileContent(t, content, 10)
	assert.True(t, errors.Is(err, domain.ErrCorruptEntry), "err=%v", err)
}

// A frame naming a codec this build did not wire is intact, not damaged, and is reported as
// a missing codec rather than as corruption.
func TestReadFile_reportsAnUnknownCodec(t *testing.T) {
	content, err := EncodeFrame(unknownCodec{}, 0, 1, entriesFrom(0, "dummy1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = readFileContent(t, content, 10)
	assert.True(t, errors.Is(err, driven.ErrUnknownCodec), "err=%v", err)
}

func TestReadFile_rejectsZeroBatchSize(t *testing.T) {
	_, err := readFileContent(t, oneFrame(t, 0, "dummy1"), 0)
	assert.NotNil(t, err)
}

func TestReadFile_hugeBatchSize(t *testing.T) {
	content := framePerEntry(t, 0, "dummy", "dummy", "dummy")
	batches, err := readFileContent(t, content, math.MaxUint32)
	assert.Nil(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 3)
}

// A read starts at the frame holding the offset asked for, which may begin earlier than it.
// The entries in front of it are decoded and dropped rather than sent.
func TestReadFile_dropsTheEntriesBeforeTheOffsetAskedFor(t *testing.T) {
	content := oneFrame(t, 0, "dummy0", "dummy1", "dummy2", "dummy3")

	batches, err := collectBatches(ReadFileParams{
		Reader:     bytes.NewReader(content),
		BatchSize:  10,
		FromOffset: 2,
		EndOffset:  math.MaxUint64,
	})

	assert.Nil(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 2)
	assert.Equal(t, uint64(2), batches[0][0].Offset)
	assert.Equal(t, "dummy2", string(batches[0][0].Entry))
	assert.Equal(t, uint64(3), batches[0][1].Offset)
}

// A frame that ends before the read begins is stepped over without being decoded.
func TestReadFile_skipsFramesBeforeTheOffsetAskedFor(t *testing.T) {
	content := framePerEntry(t, 0, "dummy0", "dummy1", "dummy2")

	batches, err := collectBatches(ReadFileParams{
		Reader:     bytes.NewReader(content),
		BatchSize:  10,
		FromOffset: 2,
		EndOffset:  math.MaxUint64,
	})

	assert.Nil(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)
	assert.Equal(t, uint64(2), batches[0][0].Offset)
}

// TestReadFile_rejectsUnexpectedFirstOffset guards the assertion that replaced the look back
// a seekable file allowed: the caller says which offset the reader starts at.
func TestReadFile_rejectsUnexpectedFirstOffset(t *testing.T) {
	_, err := collectBatches(ReadFileParams{
		Reader:     bytes.NewReader(oneFrame(t, 7, "dummy1")),
		BatchSize:  10,
		FromOffset: 5,
		EndOffset:  math.MaxUint64,
	})
	assert.NotNil(t, err)
}

func TestFindFrameByteOffset(t *testing.T) {
	content := framePerEntry(t, 0, "dummy1", "dummy2", "dummy3")
	frameSize := int64(len(content) / 3)
	store, ref, _ := blockWith(t, 0, content)
	tests := []struct {
		offsetInput        int
		byteOffsetInput    int64
		expectedByteOffset int64
		expectedScanned    int
	}{
		{offsetInput: 0, byteOffsetInput: 0, expectedByteOffset: 0, expectedScanned: 0},
		{offsetInput: 1, byteOffsetInput: 0, expectedByteOffset: frameSize, expectedScanned: 1},
		{offsetInput: 2, byteOffsetInput: 0, expectedByteOffset: 2 * frameSize, expectedScanned: 2},
		{offsetInput: 1, byteOffsetInput: frameSize, expectedByteOffset: frameSize, expectedScanned: 0},
		{offsetInput: 2, byteOffsetInput: frameSize, expectedByteOffset: 2 * frameSize, expectedScanned: 1},
		{offsetInput: 2, byteOffsetInput: 2 * frameSize, expectedByteOffset: 2 * frameSize, expectedScanned: 0},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("byteOffset %d and offset %d", test.byteOffsetInput, test.offsetInput), func(t *testing.T) {
			byteOffset, scanned, err := FindFrameByteOffset(store, ref, test.byteOffsetInput, domain.Offset(test.offsetInput))
			assert.Nil(t, err)
			assert.Equal(t, test.expectedByteOffset, byteOffset)
			assert.Equal(t, test.expectedScanned, scanned)
		})
	}
}

// Every offset a frame holds finds that frame, since a pair points at frame starts and a
// read scans inside the frame from there.
func TestFindFrameByteOffset_everyOffsetInAFrameFindsIt(t *testing.T) {
	first := oneFrame(t, 0, "dummy0", "dummy1", "dummy2")
	content := append(append([]byte(nil), first...), oneFrame(t, 3, "dummy3", "dummy4")...)
	store, ref, _ := blockWith(t, 0, content)

	for offset := 0; offset < 5; offset++ {
		want := int64(0)
		if offset >= 3 {
			want = int64(len(first))
		}
		byteOffset, _, err := FindFrameByteOffset(store, ref, 0, domain.Offset(offset))
		assert.Nil(t, err)
		assert.Equal(t, want, byteOffset, "offset %d", offset)
	}
}

func TestFindFrameByteOffset_offsetPastTheBlock(t *testing.T) {
	store, ref, _ := blockWith(t, 0, framePerEntry(t, 0, "dummy1", "dummy2"))
	_, _, err := FindFrameByteOffset(store, ref, 0, 2)
	assert.True(t, errors.Is(err, NoByteOffsetFound), "err=%v", err)
}

func TestReadFrameHeaderAt(t *testing.T) {
	content := framePerEntry(t, 0, "dummy1", "dummy2", "dummy3")
	frameSize := int64(len(content) / 3)
	store, ref, _ := blockWith(t, 0, content)

	header, err := ReadFrameHeaderAt(store, ref, frameSize)

	assert.Nil(t, err)
	assert.Equal(t, domain.Offset(1), header.FirstOffset)
	assert.Equal(t, uint32(1), header.EntryCount)
	assert.Equal(t, frameSize, header.Size())
	assert.Equal(t, uint8(driven.CodecNone), header.Codec)
}

func TestReadFile_stopsWhenCancelled(t *testing.T) {
	var content []byte
	for i := 0; i < 100; i++ {
		content = append(content, oneFrame(t, domain.Offset(i), "dummy")...)
	}
	logChan := make(chan *[]domain.LogEntry)
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		// the consumer takes one batch and goes away
		<-logChan
		wg.Done()
		close(cancel)
	}()
	_, err := ReadFile(ReadFileParams{Reader: bytes.NewReader(content), LogChan: logChan, Wg: &wg, Cancel: cancel, BatchSize: 1, EndOffset: math.MaxUint64})
	assert.True(t, errors.Is(err, domain.ErrReadCancelled), "err=%v", err)
	wg.Wait()
}

package access

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/blockstore/aferostore"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
)

var errInjected = errors.New("injected failure")

// newTestStore returns a store on a fresh in-memory filesystem, together with that
// filesystem for the few tests that look at the bytes behind the port.
func newTestStore(t *testing.T) (*aferostore.Store, *afero.Afero) {
	t.Helper()
	return aferostore.NewMem("tmp")
}

// faultyFs wraps an in-memory afero.Fs to count open file handles and inject write and truncate failures.
type faultyFs struct {
	afero.Fs
	openFiles     atomic.Int64
	failWrites    atomic.Bool
	failTruncates atomic.Bool
}

func newFaultyStore(t *testing.T) (*aferostore.Store, *faultyFs) {
	t.Helper()
	fs := &faultyFs{Fs: afero.NewMemMapFs()}
	afs := &afero.Afero{Fs: fs}
	if err := afs.MkdirAll("tmp", 0744); err != nil {
		t.Fatal(err)
	}
	return aferostore.New(afs, "tmp"), fs
}

func (f *faultyFs) Open(name string) (afero.File, error) {
	return f.track(f.Fs.Open(name))
}

func (f *faultyFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return f.track(f.Fs.OpenFile(name, flag, perm))
}

func (f *faultyFs) track(file afero.File, err error) (afero.File, error) {
	if err != nil {
		return nil, err
	}
	f.openFiles.Add(1)
	return &faultyFile{File: file, fs: f}, nil
}

type faultyFile struct {
	afero.File
	fs     *faultyFs
	closed atomic.Bool
}

// Write writes half of p before failing, like a crash or full disk mid-write.
func (f *faultyFile) Write(p []byte) (int, error) {
	if f.fs.failWrites.Load() {
		n, _ := f.File.Write(p[:len(p)/2])
		return n, errInjected
	}
	return f.File.Write(p)
}

func (f *faultyFile) Truncate(size int64) error {
	if f.fs.failTruncates.Load() {
		return errInjected
	}
	return f.File.Truncate(size)
}

func (f *faultyFile) Close() error {
	if f.closed.CompareAndSwap(false, true) {
		f.fs.openFiles.Add(-1)
	}
	return f.File.Close()
}

func newTestTopic(t *testing.T, store common.BlockStore, maxBlockSize int) *Topic {
	t.Helper()
	topic := NewLogTopic(common.TopicParams{Store: store, TopicName: "t", MaxBlockSize: maxBlockSize})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatalf("load: %v", err)
	}
	return topic
}

// blockBytes reads everything a block holds through the port.
func blockBytes(t *testing.T, store common.BlockStore, ref common.BlockRef) []byte {
	t.Helper()
	block, err := store.Open(ref, 0)
	if err != nil {
		t.Fatalf("open %s: %v", ref, err)
	}
	defer block.Close()
	content, err := io.ReadAll(block)
	if err != nil {
		t.Fatalf("read %s: %v", ref, err)
	}
	return content
}

// blockSize is the size the store reports for a block.
func blockSize(t *testing.T, store common.BlockStore, ref common.BlockRef) int64 {
	t.Helper()
	blocks, err := store.List(ref.Topic, ref.Kind)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range blocks {
		if block.Block == ref.Block {
			return block.Size
		}
	}
	t.Fatalf("%s is not in %v", ref, blocks)
	return 0
}

func payloads(from, count int) [][]byte {
	batch := make([][]byte, count)
	for i := range batch {
		batch[i] = propertyPayload(from + i)
	}
	return batch
}

func writeEntries(t *testing.T, topic *Topic, from, count int) {
	t.Helper()
	batch := payloads(from, count)
	if err := topic.Write(&batch); err != nil {
		t.Fatalf("write offsets %d..%d: %v", from, from+count-1, err)
	}
	topic.indexWg.Wait()
}

func assertReadsFromEveryOffset(t *testing.T, topic *Topic, n int) {
	t.Helper()
	for from := 0; from < n; from++ {
		got, err := readAllFrom(topic, common.Offset(from), 7)
		if err == nil && len(got) != n-from {
			err = fmt.Errorf("read %d entries, want %d", len(got), n-from)
		}
		if err == nil {
			err = checkContiguous(got, from)
		}
		if err != nil {
			t.Fatalf("from=%d: %v", from, err)
		}
	}
}

// assertIndexMatchesFullScan checks every log block is indexed and each index block equals a fresh scan of its log block.
func assertIndexMatchesFullScan(t *testing.T, topic *Topic) {
	t.Helper()
	if len(topic.IndexBlockList) != len(topic.LogBlockList) {
		t.Fatalf("indexed blocks %v, log blocks %v", topic.IndexBlockList, topic.LogBlockList)
	}
	for _, block := range topic.IndexBlockList {
		logBlock, err := topic.Store.Open(topic.logRef(common.LogBlock(block)), 0)
		if err != nil {
			t.Fatal(err)
		}
		want, _, err := index.CreateBinaryIndexFromLog(logBlock, 0, indexSparsity)
		logBlock.Close()
		if err != nil {
			t.Fatal(err)
		}
		got := blockBytes(t, topic.Store, topic.indexRef(block))
		if !bytes.Equal(got, want) {
			t.Fatalf("index of block %d:\n got %v\nwant %v", block, index.NewIndex(got).IndexOffsets, index.NewIndex(want).IndexOffsets)
		}
	}
}

func TestTopic_ReloadNeverWrittenTopic(t *testing.T) {
	store, _ := newTestStore(t)
	newTestTopic(t, store, 500) // creates the topic, as reading an unknown topic does
	topic := newTestTopic(t, store, 500)
	writeEntries(t, topic, 0, 5)
	assertReadsFromEveryOffset(t, topic, 5)
}

func TestTopic_RecoverTornTail(t *testing.T) {
	appendBytes := func(tail []byte) func(*testing.T, common.BlockStore, common.BlockRef) {
		return func(t *testing.T, store common.BlockStore, ref common.BlockRef) {
			if _, err := store.Append(ref, tail); err != nil {
				t.Fatal(err)
			}
		}
	}
	flipLastByte := func(t *testing.T, store common.BlockStore, ref common.BlockRef) {
		content := blockBytes(t, store, ref)
		content[len(content)-1] ^= 0xff
		if err := store.Truncate(ref, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ref, content); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		damage func(*testing.T, common.BlockStore, common.BlockRef)
		lost   int
	}{
		{name: "partial entry", damage: appendBytes(common.CreateByteEntry(propertyPayload(0), 0)[:15])},
		{name: "garbage", damage: appendBytes(bytes.Repeat([]byte{0xff}, 40))},
		{name: "corrupt last entry", damage: flipLastByte, lost: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _ := newTestStore(t)
			topic := newTestTopic(t, store, 2000)
			n := writeRandomBatches(t, topic, rand.New(rand.NewSource(7)), 300)
			// end on an indexed offset, so losing the last entry must also drop an index pair
			for (n-1)%10 != 0 {
				writeEntries(t, topic, n, 1)
				n++
			}
			topic.indexWg.Wait()
			if _, err := topic.UpdateIndex(); err != nil {
				t.Fatal(err)
			}
			head, _ := topic.logBlockHead()
			if indexHead, _ := topic.indexBlockHead(); uint64(indexHead) != uint64(head) {
				t.Fatalf("setup: head block %d is not indexed, index head is %d", head, indexHead)
			}
			headRef := topic.logRef(head)
			test.damage(t, store, headRef)

			topic = newTestTopic(t, store, 2000)
			n -= test.lost
			if topic.NextOffset != common.Offset(n) {
				t.Fatalf("NextOffset=%d, want %d", topic.NextOffset, n)
			}
			if size := blockSize(t, store, headRef); size != int64(topic.HeadBlockSize) {
				t.Fatalf("head block is %d bytes, HeadBlockSize=%d", size, topic.HeadBlockSize)
			}
			assertIndexMatchesFullScan(t, topic)

			writeEntries(t, topic, n, 25)
			n += 25
			if _, err := topic.UpdateIndex(); err != nil {
				t.Fatal(err)
			}
			assertIndexMatchesFullScan(t, topic)
			assertReadsFromEveryOffset(t, topic, n)
			assertReadsFromEveryOffset(t, newTestTopic(t, store, 2000), n)
		})
	}
}

func TestTopic_IncrementalIndexMatchesFullIndex(t *testing.T) {
	store, _ := newTestStore(t)
	topic := newTestTopic(t, store, 500)
	rng := rand.New(rand.NewSource(3))
	n := 0
	for i := 0; i < 60; i++ {
		size := 1 + rng.Intn(15)
		writeEntries(t, topic, n, size)
		n += size
		if i == 30 {
			topic = newTestTopic(t, store, 500)
		}
	}
	if _, err := topic.UpdateIndex(); err != nil {
		t.Fatal(err)
	}
	assertIndexMatchesFullScan(t, topic)
	assertReadsFromEveryOffset(t, topic, n)
}

func TestTopic_FailedWriteIsRolledBack(t *testing.T) {
	for _, maxBlockSize := range []int{500, 1 << 20} {
		t.Run(fmt.Sprintf("block=%d", maxBlockSize), func(t *testing.T) {
			store, fs := newFaultyStore(t)
			topic := newTestTopic(t, store, maxBlockSize)
			writeEntries(t, topic, 0, 30)

			fs.failWrites.Store(true)
			batch := payloads(30, 10)
			if err := topic.Write(&batch); !errors.Is(err, errInjected) {
				t.Fatalf("write err=%v, want injected failure", err)
			}
			fs.failWrites.Store(false)

			writeEntries(t, topic, 30, 30)
			assertReadsFromEveryOffset(t, topic, 60)
			assertReadsFromEveryOffset(t, newTestTopic(t, store, maxBlockSize), 60)
		})
	}
}

func TestTopic_WritesRefusedAfterFailedRollback(t *testing.T) {
	store, fs := newFaultyStore(t)
	topic := newTestTopic(t, store, 1<<20)
	writeEntries(t, topic, 0, 30)

	fs.failWrites.Store(true)
	fs.failTruncates.Store(true)
	batch := payloads(30, 10)
	err := topic.Write(&batch)
	if err == nil {
		t.Fatal("write succeeded despite injected failure")
	}
	if !errors.Is(err, common.ErrDirtyBlock) {
		t.Fatalf("write err=%v, want ErrDirtyBlock", err)
	}
	fs.failWrites.Store(false)
	fs.failTruncates.Store(false)

	if err := topic.Write(&batch); err == nil {
		t.Fatal("write accepted after a failed rollback")
	}
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	// complete entries from the failed batch survive recovery, the partial one does not
	recovered := int(topic.NextOffset)
	if recovered < 30 || recovered >= 40 {
		t.Fatalf("NextOffset=%d after recovery, want within [30, 40)", recovered)
	}
	writeEntries(t, topic, recovered, 40-recovered)
	assertReadsFromEveryOffset(t, topic, 40)
}

func TestTopic_WriteToUnwritableBlockReturnsError(t *testing.T) {
	mem := afero.NewMemMapFs()
	if err := mem.MkdirAll("tmp/t", 0744); err != nil {
		t.Fatal(err)
	}
	store := aferostore.New(&afero.Afero{Fs: afero.NewReadOnlyFs(mem)}, "tmp")
	topic := newTestTopic(t, store, 500)
	batch := payloads(0, 3)
	if err := topic.Write(&batch); err == nil {
		t.Fatal("write to a read-only filesystem succeeded")
	}
}

func TestTopic_ClosesFileHandles(t *testing.T) {
	store, fs := newFaultyStore(t)
	topic := newTestTopic(t, store, 500)
	n := writeRandomBatches(t, topic, rand.New(rand.NewSource(5)), 200)
	topic.indexWg.Wait()
	if _, err := topic.UpdateIndex(); err != nil {
		t.Fatal(err)
	}
	assertReadsFromEveryOffset(t, topic, n)
	assertReadsFromEveryOffset(t, newTestTopic(t, store, 500), n)
	if open := fs.openFiles.Load(); open != 0 {
		t.Fatalf("%d file handles left open", open)
	}
}

func TestTopic_ReadFailsOnCorruptEntry(t *testing.T) {
	store, _ := newTestStore(t)
	topic := newTestTopic(t, store, 500)
	writeRandomBatches(t, topic, rand.New(rand.NewSource(9)), 100)
	topic.indexWg.Wait()
	firstBlock := topic.logRef(topic.LogBlockList[0])
	content := blockBytes(t, store, firstBlock)
	content[common.EntryOverhead+len(propertyPayload(0))+12] ^= 0xff // first payload byte of offset 1
	if err := store.Truncate(firstBlock, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(firstBlock, content); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, common.ErrCorruptEntry) {
		t.Fatalf("err=%v, want ErrCorruptEntry", err)
	}
}

func TestTopic_ReadRejectsZeroBatchSize(t *testing.T) {
	store, _ := newTestStore(t)
	topic := newTestTopic(t, store, 500)
	writeEntries(t, topic, 0, 3)
	if _, err := readAllFrom(topic, 0, 0); err == nil {
		t.Fatal("read with batch size 0 succeeded")
	}
}

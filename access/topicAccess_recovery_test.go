package access

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/access/index"
)

var errInjected = errors.New("injected failure")

// faultyFs wraps an in-memory afero.Fs to count open file handles and inject write and truncate failures.
type faultyFs struct {
	afero.Fs
	openFiles     atomic.Int64
	failWrites    atomic.Bool
	failTruncates atomic.Bool
}

func newFaultyAfs() (*afero.Afero, *faultyFs) {
	fs := &faultyFs{Fs: afero.NewMemMapFs()}
	return &afero.Afero{Fs: fs}, fs
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

func newTestTopic(t *testing.T, afs *afero.Afero, maxBlockSize int) *Topic {
	t.Helper()
	topic := NewLogTopic(common.TopicParams{Afs: afs, RootPath: "tmp", TopicName: "t", MaxBlockSize: maxBlockSize})
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatalf("load: %v", err)
	}
	return topic
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

// assertIndexMatchesFullScan checks every log block is indexed and each index file equals a fresh scan of its block.
func assertIndexMatchesFullScan(t *testing.T, topic *Topic) {
	t.Helper()
	if len(topic.IndexBlockList) != len(topic.LogBlockList) {
		t.Fatalf("indexed blocks %v, log blocks %v", topic.IndexBlockList, topic.LogBlockList)
	}
	for _, block := range topic.IndexBlockList {
		logFile, _ := topic.logBlockFileName(common.LogBlock(block))
		want, _, err := index.CreateBinaryIndexFromLogFile(topic.Afs, logFile, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		indexFile, _ := topic.indexBlockFileName(block)
		got, err := topic.Afs.ReadFile(indexFile)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("index of block %d:\n got %v\nwant %v", block, index.NewIndex(got).IndexOffsets, index.NewIndex(want).IndexOffsets)
		}
	}
}

func TestTopic_ReloadNeverWrittenTopic(t *testing.T) {
	afs := common.MemAfs()
	newTestTopic(t, afs, 500) // creates the topic directory, as reading an unknown topic does
	topic := newTestTopic(t, afs, 500)
	writeEntries(t, topic, 0, 5)
	assertReadsFromEveryOffset(t, topic, 5)
}

func TestTopic_RecoverTornTail(t *testing.T) {
	appendBytes := func(tail []byte) func(*testing.T, *afero.Afero, string) {
		return func(t *testing.T, afs *afero.Afero, name string) {
			file, err := afs.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(tail); err != nil {
				t.Fatal(err)
			}
		}
	}
	flipLastByte := func(t *testing.T, afs *afero.Afero, name string) {
		content, err := afs.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		content[len(content)-1] ^= 0xff
		if err := afs.WriteFile(name, content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		damage func(*testing.T, *afero.Afero, string)
		lost   int
	}{
		{name: "partial entry", damage: appendBytes(common.CreateByteEntry(propertyPayload(0), 0)[:15])},
		{name: "garbage", damage: appendBytes(bytes.Repeat([]byte{0xff}, 40))},
		{name: "corrupt last entry", damage: flipLastByte, lost: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			afs := common.MemAfs()
			topic := newTestTopic(t, afs, 2000)
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
			headFile, _ := topic.logBlockFileName(head)
			test.damage(t, afs, headFile)

			topic = newTestTopic(t, afs, 2000)
			n -= test.lost
			if topic.NextOffset != common.Offset(n) {
				t.Fatalf("NextOffset=%d, want %d", topic.NextOffset, n)
			}
			info, err := afs.Stat(headFile)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != int64(topic.HeadBlockSize) {
				t.Fatalf("head block is %d bytes, HeadBlockSize=%d", info.Size(), topic.HeadBlockSize)
			}
			assertIndexMatchesFullScan(t, topic)

			writeEntries(t, topic, n, 25)
			n += 25
			if _, err := topic.UpdateIndex(); err != nil {
				t.Fatal(err)
			}
			assertIndexMatchesFullScan(t, topic)
			assertReadsFromEveryOffset(t, topic, n)
			assertReadsFromEveryOffset(t, newTestTopic(t, afs, 2000), n)
		})
	}
}

func TestTopic_IncrementalIndexMatchesFullIndex(t *testing.T) {
	afs := common.MemAfs()
	topic := newTestTopic(t, afs, 500)
	rng := rand.New(rand.NewSource(3))
	n := 0
	for i := 0; i < 60; i++ {
		size := 1 + rng.Intn(15)
		writeEntries(t, topic, n, size)
		n += size
		if i == 30 {
			topic = newTestTopic(t, afs, 500)
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
			afs, fs := newFaultyAfs()
			topic := newTestTopic(t, afs, maxBlockSize)
			writeEntries(t, topic, 0, 30)

			fs.failWrites.Store(true)
			batch := payloads(30, 10)
			if err := topic.Write(&batch); !errors.Is(err, errInjected) {
				t.Fatalf("write err=%v, want injected failure", err)
			}
			fs.failWrites.Store(false)

			writeEntries(t, topic, 30, 30)
			assertReadsFromEveryOffset(t, topic, 60)
			assertReadsFromEveryOffset(t, newTestTopic(t, afs, maxBlockSize), 60)
		})
	}
}

func TestTopic_WritesRefusedAfterFailedRollback(t *testing.T) {
	afs, fs := newFaultyAfs()
	topic := newTestTopic(t, afs, 1<<20)
	writeEntries(t, topic, 0, 30)

	fs.failWrites.Store(true)
	fs.failTruncates.Store(true)
	batch := payloads(30, 10)
	if err := topic.Write(&batch); err == nil {
		t.Fatal("write succeeded despite injected failure")
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
	topic := newTestTopic(t, &afero.Afero{Fs: afero.NewReadOnlyFs(mem)}, 500)
	batch := payloads(0, 3)
	if err := topic.Write(&batch); err == nil {
		t.Fatal("write to a read-only filesystem succeeded")
	}
}

func TestTopic_ClosesFileHandles(t *testing.T) {
	afs, fs := newFaultyAfs()
	topic := newTestTopic(t, afs, 500)
	n := writeRandomBatches(t, topic, rand.New(rand.NewSource(5)), 200)
	topic.indexWg.Wait()
	if _, err := topic.UpdateIndex(); err != nil {
		t.Fatal(err)
	}
	assertReadsFromEveryOffset(t, topic, n)
	assertReadsFromEveryOffset(t, newTestTopic(t, afs, 500), n)
	if open := fs.openFiles.Load(); open != 0 {
		t.Fatalf("%d file handles left open", open)
	}
}

func TestTopic_ReadFailsOnCorruptEntry(t *testing.T) {
	afs := common.MemAfs()
	topic := newTestTopic(t, afs, 500)
	writeRandomBatches(t, topic, rand.New(rand.NewSource(9)), 100)
	topic.indexWg.Wait()
	firstBlock, _ := topic.logBlockFileName(topic.LogBlockList[0])
	content, err := afs.ReadFile(firstBlock)
	if err != nil {
		t.Fatal(err)
	}
	content[common.EntryOverhead+len(propertyPayload(0))+12] ^= 0xff // first payload byte of offset 1
	if err := afs.WriteFile(firstBlock, content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllFrom(topic, 0, 10); !errors.Is(err, common.ErrCorruptEntry) {
		t.Fatalf("err=%v, want ErrCorruptEntry", err)
	}
}

func TestTopic_ReadRejectsZeroBatchSize(t *testing.T) {
	topic := newTestTopic(t, common.MemAfs(), 500)
	writeEntries(t, topic, 0, 3)
	if _, err := readAllFrom(topic, 0, 0); err == nil {
		t.Fatal("read with batch size 0 succeeded")
	}
}

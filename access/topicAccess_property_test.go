package access

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
)

// Property tests against today's Topic. They use only the stdlib so they can
// later become the shared conformance suite for BlockStore adapters.

func propertyPayload(offset int) []byte {
	return []byte(fmt.Sprintf("e%d-%s", offset, strings.Repeat("x", offset%37)))
}

func readAllFrom(topic *Topic, from common.Offset, batchSize uint32) ([]common.LogEntry, error) {
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	var got []common.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got = append(got, *batch...)
			wg.Done()
		}
		close(done)
	}()
	err := topic.Read(common.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: batchSize})
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

// checkContiguous verifies got starts at from, has no gaps, and carries the payload written at each offset.
func checkContiguous(got []common.LogEntry, from int) error {
	for k, entry := range got {
		if entry.Offset != uint64(from+k) {
			return fmt.Errorf("entry %d has offset %d, want %d", k, entry.Offset, from+k)
		}
		if string(entry.Entry) != string(propertyPayload(from+k)) {
			return fmt.Errorf("offset %d has payload %q", entry.Offset, entry.Entry)
		}
	}
	return nil
}

func writeRandomBatches(t *testing.T, topic *Topic, rng *rand.Rand, total int) int {
	written := 0
	for written < total {
		batch := make([][]byte, 1+rng.Intn(40))
		for j := range batch {
			batch[j] = propertyPayload(written + j)
		}
		if err := topic.Write(&batch); err != nil {
			t.Fatalf("write: %v", err)
		}
		written += len(batch)
	}
	return written
}

// removeIndexFiles deletes every .idx file in dir, or only the newest one.
func removeIndexFiles(t *testing.T, afs *afero.Afero, dir string, newestOnly bool) {
	infos, err := afs.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var indexFiles []string
	for _, info := range infos {
		if strings.HasSuffix(info.Name(), ".idx") {
			indexFiles = append(indexFiles, info.Name())
		}
	}
	if newestOnly && len(indexFiles) > 0 {
		indexFiles = indexFiles[len(indexFiles)-1:]
	}
	for _, name := range indexFiles {
		if err := afs.Remove(dir + common.Sep + name); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopicProperty_ReadFromEveryOffset(t *testing.T) {
	const total = 300
	for _, maxBlockSize := range []int{64, 500, 2000, 1 << 20} {
		for _, mode := range []string{"live", "reload", "reload-without-index", "reload-without-newest-index"} {
			t.Run(fmt.Sprintf("block=%d/%s", maxBlockSize, mode), func(t *testing.T) {
				afs := common.MemAfs()
				params := common.TopicParams{Afs: afs, RootPath: "tmp", TopicName: "t", MaxBlockSize: maxBlockSize}
				topic := NewLogTopic(params)
				_ = topic.LoadOrCreate()
				n := writeRandomBatches(t, topic, rand.New(rand.NewSource(int64(maxBlockSize))), total)
				topic.indexWg.Wait()

				if maxBlockSize < 1000 && len(topic.LogBlockList) < 2 {
					t.Fatalf("expected multiple blocks, got %v", topic.LogBlockList)
				}
				if mode != "live" {
					if mode == "reload-without-index" || mode == "reload-without-newest-index" {
						removeIndexFiles(t, afs, "tmp"+common.Sep+"t", mode == "reload-without-newest-index")
					}
					topic = NewLogTopic(params)
					if err := topic.LoadOrCreate(); err != nil {
						t.Fatalf("reload: %v", err)
					}
				}
				if topic.NextOffset != common.Offset(n) {
					t.Fatalf("NextOffset=%d, want %d", topic.NextOffset, n)
				}

				failures := 0
				for from := 0; from < n && failures < 5; from++ {
					for _, batchSize := range []uint32{1, 7, 1000} {
						got, err := readAllFrom(topic, common.Offset(from), batchSize)
						if err == nil && len(got) != n-from {
							err = fmt.Errorf("read %d entries, want %d", len(got), n-from)
						}
						if err == nil {
							err = checkContiguous(got, from)
						}
						if err != nil {
							failures++
							t.Errorf("from=%d batchSize=%d: %v", from, batchSize, err)
							break
						}
					}
				}

				if _, err := readAllFrom(topic, common.Offset(n), 10); err != common.NoEntriesFound {
					t.Errorf("read past end: err=%v, want NoEntriesFound", err)
				}
			})
		}
	}
}

// Run with -race: one writer, the background indexer, and concurrent readers.
func TestTopicProperty_ConcurrentWriteReadIndex(t *testing.T) {
	afs := common.MemAfs()
	params := common.TopicParams{Afs: afs, RootPath: "tmp", TopicName: "t", MaxBlockSize: 500}
	topic := NewLogTopic(params)
	_ = topic.LoadOrCreate()

	var committed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := topic.UpdateIndex(); err != nil {
					t.Errorf("index: %v", err)
					return
				}
			}
		}
	}()

	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := int(committed.Load())
				if n == 0 {
					continue
				}
				from := rng.Intn(n)
				got, err := readAllFrom(topic, common.Offset(from), uint32(1+rng.Intn(50)))
				if err == nil && len(got) < n-from {
					err = fmt.Errorf("read %d entries, want at least %d", len(got), n-from)
				}
				if err == nil {
					err = checkContiguous(got, from)
				}
				if err != nil {
					t.Errorf("from=%d: %v", from, err)
					return
				}
			}
		}(int64(r))
	}

	rng := rand.New(rand.NewSource(42))
	written := 0
	for i := 0; i < 200; i++ {
		batch := make([][]byte, 1+rng.Intn(20))
		for j := range batch {
			batch[j] = propertyPayload(written + j)
		}
		if err := topic.Write(&batch); err != nil {
			t.Fatalf("write: %v", err)
		}
		written += len(batch)
		committed.Store(int64(written))
	}
	close(stop)
	wg.Wait()
	topic.indexWg.Wait()

	reloaded := NewLogTopic(params)
	if err := reloaded.LoadOrCreate(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, err := readAllFrom(reloaded, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != written {
		t.Fatalf("reloaded read %d entries, want %d", len(got), written)
	}
	if err := checkContiguous(got, 0); err != nil {
		t.Fatal(err)
	}
}

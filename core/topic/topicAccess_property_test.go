package topic

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driven/blockstore/flashstore"
	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// Property tests of the core, run against every BlockStore adapter. Only the factory below
// knows which backend is behind the port; everything else speaks in topics and offsets.

type coreBackend struct {
	name     string
	newStore func(t *testing.T) driven.BlockStore
}

// coreBackends are the stores the core is checked against: the two filesystem adapters, the
// in-memory one, which offers nothing beyond the port itself, and the flash one, whose pages,
// fixed capacity and write-once bytes are as far from a file as the port goes.
func coreBackends() []coreBackend {
	return []coreBackend{
		{name: "afero", newStore: func(t *testing.T) driven.BlockStore {
			store, _ := newTestStore(t)
			return store
		}},
		// on a real directory, which is the only filesystem this one has
		{name: "file", newStore: func(t *testing.T) driven.BlockStore {
			return filestore.NewOS(t.TempDir())
		}},
		{name: "mem", newStore: func(t *testing.T) driven.BlockStore {
			return memstore.New()
		}},
		{name: "flash", newStore: func(t *testing.T) driven.BlockStore {
			store, err := flashstore.New(flashstore.NewRAMDevice(4096, 256))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
}

func propertyPayload(offset int) []byte {
	return []byte(fmt.Sprintf("e%d-%s", offset, strings.Repeat("x", offset%37)))
}

func readAllFrom(topic *Topic, from domain.Offset, batchSize uint32) ([]domain.LogEntry, error) {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var got []domain.LogEntry
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got = append(got, *batch...)
			wg.Done()
		}
		close(done)
	}()
	err := topic.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: batchSize})
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

// checkContiguous verifies got starts at from, has no gaps, and carries the payload written at each offset.
func checkContiguous(got []domain.LogEntry, from int) error {
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

// removeIndexBlocks deletes every index block of a topic, or only the newest one.
func removeIndexBlocks(t *testing.T, store driven.BlockStore, topic domain.TopicName, newestOnly bool) {
	t.Helper()
	blocks, err := store.List(topic, driven.Index)
	if err != nil {
		t.Fatal(err)
	}
	if newestOnly && len(blocks) > 0 {
		blocks = blocks[len(blocks)-1:]
	}
	for _, block := range blocks {
		if err := store.Remove(driven.IndexRef(topic, domain.IndexBlock(block.Block))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopicProperty_ReadFromEveryOffset(t *testing.T) {
	for _, backend := range coreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			readFromEveryOffset(t, backend)
		})
	}
}

func readFromEveryOffset(t *testing.T, backend coreBackend) {
	const total = 300
	for _, maxBlockSize := range []int{64, 500, 2000, 1 << 20} {
		for _, mode := range []string{"live", "reload", "reload-without-index", "reload-without-newest-index"} {
			t.Run(fmt.Sprintf("block=%d/%s", maxBlockSize, mode), func(t *testing.T) {
				store := backend.newStore(t)
				params := Params{Store: store, TopicName: "t", MaxBlockSize: maxBlockSize}
				topic := NewLogTopic(params)
				_ = topic.LoadOrCreate()
				n := writeRandomBatches(t, topic, rand.New(rand.NewSource(int64(maxBlockSize))), total)
				topic.indexWg.Wait()

				if maxBlockSize < 1000 && len(topic.LogBlockList) < 2 {
					t.Fatalf("expected multiple blocks, got %v", topic.LogBlockList)
				}
				if mode != "live" {
					if mode == "reload-without-index" || mode == "reload-without-newest-index" {
						removeIndexBlocks(t, store, "t", mode == "reload-without-newest-index")
					}
					topic = NewLogTopic(params)
					if err := topic.LoadOrCreate(); err != nil {
						t.Fatalf("reload: %v", err)
					}
				}
				if topic.NextOffset != domain.Offset(n) {
					t.Fatalf("NextOffset=%d, want %d", topic.NextOffset, n)
				}

				failures := 0
				for from := 0; from < n && failures < 5; from++ {
					for _, batchSize := range []uint32{1, 7, 1000} {
						got, err := readAllFrom(topic, domain.Offset(from), batchSize)
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

				if _, err := readAllFrom(topic, domain.Offset(n), 10); err != domain.NoEntriesFound {
					t.Errorf("read past end: err=%v, want NoEntriesFound", err)
				}
			})
		}
	}
}

// Run with -race: one writer, the background indexer, and concurrent readers.
func TestTopicProperty_ConcurrentWriteReadIndex(t *testing.T) {
	for _, backend := range coreBackends() {
		t.Run(backend.name, func(t *testing.T) {
			concurrentWriteReadIndex(t, backend)
		})
	}
}

func concurrentWriteReadIndex(t *testing.T, backend coreBackend) {
	store := backend.newStore(t)
	params := Params{Store: store, TopicName: "t", MaxBlockSize: 500}
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
				got, err := readAllFrom(topic, domain.Offset(from), uint32(1+rng.Intn(50)))
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

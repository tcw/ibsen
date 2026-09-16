// Package conformance is the shared property suite every BlockStore adapter has to pass.
// It is the port's contract written down once, so an exotic backend is trusted for the same
// reason the filesystem one is: it passes the same tests.
package conformance

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/tcw/ibsen/access/common"
)

// NewStore makes an empty store for one test. Each call must give a store that shares
// nothing with the ones before it.
type NewStore func(t *testing.T) common.BlockStore

// Run checks a store against the whole contract. Run it with -race as well: a store must be
// safe for concurrent use.
func Run(t *testing.T, newStore NewStore) {
	t.Helper()
	tests := []struct {
		name string
		run  func(t *testing.T, store common.BlockStore)
	}{
		{"RoundTrip", roundTrip},
		{"ReadFromEveryByteOffset", readFromEveryByteOffset},
		{"AppendCreatesTopicAndBlock", appendCreatesTopicAndBlock},
		{"AppendOfNoBytesCreatesTheBlock", appendOfNoBytesCreatesTheBlock},
		{"CreateTopicReportsCreation", createTopicReportsCreation},
		{"TopicsListsEveryTopic", topicsListsEveryTopic},
		{"ListSeparatesKindsAndOrdersBlocks", listSeparatesKindsAndOrdersBlocks},
		{"ListOfUnknownTopicIsEmpty", listOfUnknownTopicIsEmpty},
		{"UnknownBlockIsAnError", unknownBlockIsAnError},
		{"TruncateRejectsSizeLargerThanBlock", truncateRejectsSizeLargerThanBlock},
		{"TruncateThenAppend", truncateThenAppend},
		{"RemoveDropsOnlyItsBlock", removeDropsOnlyItsBlock},
		{"ReaderKeepsReadingAcrossAppends", readerKeepsReadingAcrossAppends},
		{"SyncIsOptional", syncIsOptional},
		{"ConcurrentAppendsAndReads", concurrentAppendsAndReads},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t, newStore(t))
		})
	}
}

func mustAppend(t *testing.T, store common.BlockStore, ref common.BlockRef, data []byte) common.Block {
	t.Helper()
	block, err := store.Append(ref, data)
	if err != nil {
		t.Fatalf("append to %s: %v", ref, err)
	}
	if block.Block != ref.Block {
		t.Fatalf("append to %s returned block %d", ref, block.Block)
	}
	return block
}

func read(t *testing.T, store common.BlockStore, ref common.BlockRef, byteOffset int64) []byte {
	t.Helper()
	block, err := store.Open(ref, byteOffset)
	if err != nil {
		t.Fatalf("open %s at %d: %v", ref, byteOffset, err)
	}
	defer block.Close()
	content, err := io.ReadAll(block)
	if err != nil {
		t.Fatalf("read %s at %d: %v", ref, byteOffset, err)
	}
	return content
}

func sizeOf(t *testing.T, store common.BlockStore, ref common.BlockRef) int64 {
	t.Helper()
	blocks, err := store.List(ref.Topic, ref.Kind)
	if err != nil {
		t.Fatalf("list %s: %v", ref.Topic, err)
	}
	for _, block := range blocks {
		if block.Block == ref.Block {
			return block.Size
		}
	}
	t.Fatalf("%s is not among %v", ref, blocks)
	return 0
}

func roundTrip(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	var want []byte
	for _, chunk := range []string{"first", "", "second", "third"} {
		want = append(want, chunk...)
		block := mustAppend(t, store, ref, []byte(chunk))
		if block.Size != int64(len(want)) {
			t.Fatalf("append returned size %d, want %d", block.Size, len(want))
		}
	}
	if got := read(t, store, ref, 0); !bytes.Equal(got, want) {
		t.Fatalf("read %q, want %q", got, want)
	}
	if size := sizeOf(t, store, ref); size != int64(len(want)) {
		t.Fatalf("list reports %d bytes, want %d", size, len(want))
	}
}

func readFromEveryByteOffset(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	want := []byte("the store is a byte range addressed from any point in it")
	mustAppend(t, store, ref, want)
	for from := 0; from <= len(want); from++ {
		if got := read(t, store, ref, int64(from)); !bytes.Equal(got, want[from:]) {
			t.Fatalf("read from %d gave %q, want %q", from, got, want[from:])
		}
	}
	// a byte offset past the end is an empty read, not an error
	if got := read(t, store, ref, int64(len(want))+10); len(got) != 0 {
		t.Fatalf("read past the end gave %q", got)
	}
}

func appendCreatesTopicAndBlock(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("never-created", 42)
	mustAppend(t, store, ref, []byte("x"))
	topics, err := store.Topics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0] != "never-created" {
		t.Fatalf("topics=%v, want the topic the append created", topics)
	}
	if got := read(t, store, ref, 0); string(got) != "x" {
		t.Fatalf("read %q", got)
	}
}

func appendOfNoBytesCreatesTheBlock(t *testing.T, store common.BlockStore) {
	ref := common.IndexRef("topic", 0)
	mustAppend(t, store, ref, nil)
	blocks, err := store.List("topic", common.Index)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].Size != 0 {
		t.Fatalf("index blocks=%v, want one empty block", blocks)
	}
	if got := read(t, store, ref, 0); len(got) != 0 {
		t.Fatalf("read %q from an empty block", got)
	}
}

func createTopicReportsCreation(t *testing.T, store common.BlockStore) {
	created, err := store.CreateTopic("topic")
	if err != nil || !created {
		t.Fatalf("first create: created=%v, err=%v", created, err)
	}
	created, err = store.CreateTopic("topic")
	if err != nil || created {
		t.Fatalf("second create: created=%v, err=%v", created, err)
	}
	blocks, err := store.List("topic", common.Log)
	if err != nil || len(blocks) != 0 {
		t.Fatalf("a new topic holds %v, err=%v", blocks, err)
	}
}

func topicsListsEveryTopic(t *testing.T, store common.BlockStore) {
	if _, err := store.CreateTopic("created"); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, store, common.LogRef("appended", 0), []byte("x"))
	topics, err := store.Topics()
	if err != nil {
		t.Fatal(err)
	}
	found := map[common.TopicName]bool{}
	for _, topic := range topics {
		found[topic] = true
	}
	if !found["created"] || !found["appended"] || len(topics) != 2 {
		t.Fatalf("topics=%v", topics)
	}
}

func listSeparatesKindsAndOrdersBlocks(t *testing.T, store common.BlockStore) {
	for _, block := range []common.LogBlock{42, 0, 7} {
		mustAppend(t, store, common.LogRef("topic", block), []byte("log"))
	}
	mustAppend(t, store, common.IndexRef("topic", 7), []byte("indexed"))
	logs, err := store.List("topic", common.Log)
	if err != nil {
		t.Fatal(err)
	}
	want := []common.Block{{Block: 0, Size: 3}, {Block: 7, Size: 3}, {Block: 42, Size: 3}}
	if fmt.Sprint(logs) != fmt.Sprint(want) {
		t.Fatalf("log blocks=%v, want %v", logs, want)
	}
	indexes, err := store.List("topic", common.Index)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0] != (common.Block{Block: 7, Size: 7}) {
		t.Fatalf("index blocks=%v, want block 7 of 7 bytes", indexes)
	}
}

func listOfUnknownTopicIsEmpty(t *testing.T, store common.BlockStore) {
	blocks, err := store.List("never-heard-of-it", common.Log)
	if err != nil {
		t.Fatalf("listing an unknown topic failed: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("an unknown topic holds %v", blocks)
	}
}

func unknownBlockIsAnError(t *testing.T, store common.BlockStore) {
	// a topic that exists, so it is the block that is missing rather than everything
	mustAppend(t, store, common.LogRef("topic", 0), []byte("x"))
	missing := common.LogRef("topic", 1)
	if _, err := store.Open(missing, 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Errorf("open: err=%v, want ErrBlockNotFound", err)
	}
	if err := store.Truncate(missing, 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Errorf("truncate: err=%v, want ErrBlockNotFound", err)
	}
	if err := store.Remove(missing); !errors.Is(err, common.ErrBlockNotFound) {
		t.Errorf("remove: err=%v, want ErrBlockNotFound", err)
	}
	if _, err := store.Open(common.LogRef("no-such-topic", 0), 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Errorf("open in an unknown topic: err=%v, want ErrBlockNotFound", err)
	}
}

func truncateRejectsSizeLargerThanBlock(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, []byte("hello"))
	if err := store.Truncate(ref, 6); !errors.Is(err, common.ErrInvalidSize) {
		t.Fatalf("truncate past the end: err=%v, want ErrInvalidSize", err)
	}
	if got := read(t, store, ref, 0); string(got) != "hello" {
		t.Fatalf("a rejected truncate changed the block to %q", got)
	}
}

func truncateThenAppend(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, []byte("hello world"))
	if err := store.Truncate(ref, 5); err != nil {
		t.Fatal(err)
	}
	if size := sizeOf(t, store, ref); size != 5 {
		t.Fatalf("list reports %d bytes after truncate, want 5", size)
	}
	block := mustAppend(t, store, ref, []byte("!"))
	if block.Size != 6 {
		t.Fatalf("append after truncate returned size %d, want 6", block.Size)
	}
	if got := read(t, store, ref, 0); string(got) != "hello!" {
		t.Fatalf("after truncate and append: %q", got)
	}
	if err := store.Truncate(ref, 0); err != nil {
		t.Fatal(err)
	}
	if got := read(t, store, ref, 0); len(got) != 0 {
		t.Fatalf("after truncating to nothing: %q", got)
	}
}

func removeDropsOnlyItsBlock(t *testing.T, store common.BlockStore) {
	gone := common.LogRef("topic", 0)
	kept := common.LogRef("topic", 1)
	keptIndex := common.IndexRef("topic", 0)
	mustAppend(t, store, gone, []byte("gone"))
	mustAppend(t, store, kept, []byte("kept"))
	mustAppend(t, store, keptIndex, []byte("index"))
	if err := store.Remove(gone); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(gone, 0); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("open after remove: err=%v", err)
	}
	if err := store.Remove(gone); !errors.Is(err, common.ErrBlockNotFound) {
		t.Fatalf("removing twice: err=%v, want ErrBlockNotFound", err)
	}
	if got := read(t, store, kept, 0); string(got) != "kept" {
		t.Fatalf("the other log block reads %q", got)
	}
	if got := read(t, store, keptIndex, 0); string(got) != "index" {
		t.Fatalf("the index block of the removed log block reads %q", got)
	}
}

// readerKeepsReadingAcrossAppends is what lets a slow consumer tail a block: the reader a
// read was given must stay usable while writers keep appending.
func readerKeepsReadingAcrossAppends(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, []byte("opened"))
	block, err := store.Open(ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer block.Close()
	mustAppend(t, store, ref, []byte(" and then appended to"))
	content, err := io.ReadAll(block)
	if err != nil {
		t.Fatalf("read after an append: %v", err)
	}
	// a store may or may not show the later append; it must show what was there at Open
	if !bytes.HasPrefix(content, []byte("opened")) {
		t.Fatalf("reader gave %q, want it to start with what the block held when it opened", content)
	}
}

func syncIsOptional(t *testing.T, store common.BlockStore) {
	ref := common.LogRef("topic", 0)
	mustAppend(t, store, ref, []byte("durable"))
	synced, err := common.Sync(store, ref)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if _, isSyncable := store.(common.Syncable); isSyncable != synced {
		t.Fatalf("Sync reported synced=%v for a store where Syncable is %v", synced, isSyncable)
	}
	if got := read(t, store, ref, 0); string(got) != "durable" {
		t.Fatalf("after sync the block reads %q", got)
	}
}

// concurrentAppendsAndReads is worth running with -race: a store is used by a writer, a
// background indexer and any number of readers at once.
func concurrentAppendsAndReads(t *testing.T, store common.BlockStore) {
	const writers = 4
	const appends = 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ref := common.LogRef("topic", common.LogBlock(w))
			for i := 0; i < appends; i++ {
				if _, err := store.Append(ref, []byte("x")); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < appends; i++ {
				ref := common.LogRef("topic", common.LogBlock(rng.Intn(writers)))
				block, err := store.Open(ref, 0)
				if errors.Is(err, common.ErrBlockNotFound) {
					continue
				}
				if err != nil {
					t.Errorf("open: %v", err)
					return
				}
				content, err := io.ReadAll(block)
				block.Close()
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				if len(bytes.Trim(content, "x")) != 0 {
					t.Errorf("read %q, want only the bytes appended", content)
					return
				}
			}
		}(int64(r))
	}
	wg.Wait()
	for w := 0; w < writers; w++ {
		ref := common.LogRef("topic", common.LogBlock(w))
		if size := sizeOf(t, store, ref); size != appends {
			t.Errorf("%s holds %d bytes, want %d", ref, size, appends)
		}
	}
}

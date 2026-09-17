package flashstore

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/conformance"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// a page of 128 bytes holds 64 bytes of log, so the suite's blocks span several pages
const (
	testPages    = 256
	testPageSize = 128
)

func newTestStore(t *testing.T) (*Store, *RAMDevice) {
	t.Helper()
	dev := NewRAMDevice(testPages, testPageSize)
	store, err := New(dev)
	if err != nil {
		t.Fatal(err)
	}
	return store, dev
}

func TestFlashStore_Conformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driven.BlockStore {
		store, _ := newTestStore(t)
		return store
	})
}

func read(t *testing.T, store *Store, ref driven.BlockRef, byteOffset int64) []byte {
	t.Helper()
	block, err := store.Open(ref, byteOffset)
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

// TestFlashStore_PageTableIsRebuiltFromTheRegion is what makes the adapter a store rather
// than a cache: the page table lives in RAM, and reopening the same region reconstructs it.
func TestFlashStore_PageTableIsRebuiltFromTheRegion(t *testing.T) {
	store, dev := newTestStore(t)
	want := map[driven.BlockRef][]byte{
		driven.LogRef("orders", 0):    bytes.Repeat([]byte("a"), 3*store.DataPerPage()+7),
		driven.LogRef("orders", 1000): []byte("second block"),
		driven.IndexRef("orders", 0):  []byte("index pairs"),
		driven.LogRef("payments", 0):  bytes.Repeat([]byte("b"), store.DataPerPage()),
	}
	for ref, data := range want {
		// several appends, so a block is built the way the log builds it
		for from := 0; from < len(data); from += 5 {
			to := from + 5
			if to > len(data) {
				to = len(data)
			}
			if _, err := store.Append(ref, data[from:to]); err != nil {
				t.Fatalf("append to %s: %v", ref, err)
			}
		}
	}

	reopened, err := New(dev)
	if err != nil {
		t.Fatal(err)
	}
	topics, err := reopened.Topics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 || topics[0] != "orders" || topics[1] != "payments" {
		t.Fatalf("topics after reopening: %v", topics)
	}
	for ref, data := range want {
		if got := read(t, reopened, ref, 0); !bytes.Equal(got, data) {
			t.Fatalf("%s holds %d bytes after reopening, want %d", ref, len(got), len(data))
		}
	}
	blocks, err := reopened.List("orders", driven.Log)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 || blocks[0].Block != 0 || blocks[1].Block != 1000 {
		t.Fatalf("log blocks of orders after reopening: %v", blocks)
	}
}

// TestFlashStore_ReclaimsPages: the region is fixed, so a removed block has to give its
// pages back.
func TestFlashStore_ReclaimsPages(t *testing.T) {
	store, _ := newTestStore(t)
	free := store.FreePages()
	ref := driven.LogRef("topic", 0)
	if _, err := store.Append(ref, bytes.Repeat([]byte("x"), 4*store.DataPerPage())); err != nil {
		t.Fatal(err)
	}
	if used := free - store.FreePages(); used != 4 {
		t.Fatalf("a block of four pages took %d", used)
	}
	if err := store.Truncate(ref, int64(store.DataPerPage())); err != nil {
		t.Fatal(err)
	}
	if used := free - store.FreePages(); used != 1 {
		t.Fatalf("after truncating to one page the block holds %d", used)
	}
	if err := store.Remove(ref); err != nil {
		t.Fatal(err)
	}
	if store.FreePages() != free {
		t.Fatalf("%d pages are still taken after the block was removed", free-store.FreePages())
	}
}

func TestFlashStore_FullRegion(t *testing.T) {
	dev := NewRAMDevice(4, testPageSize)
	store, err := New(dev)
	if err != nil {
		t.Fatal(err)
	}
	ref := driven.LogRef("topic", 0)
	if _, err := store.Append(ref, bytes.Repeat([]byte("x"), 4*store.DataPerPage())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ref, []byte("one byte too many")); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("append to a full region: err=%v, want ErrNoSpace", err)
	}
	// the rejected append changed nothing
	if got := read(t, store, ref, 0); len(got) != 4*store.DataPerPage() {
		t.Fatalf("the block holds %d bytes after the rejected append", len(got))
	}
	if err := store.Remove(ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ref, []byte("room again")); err != nil {
		t.Fatalf("append after freeing the region: %v", err)
	}
}

// TestFlashStore_AppendOnlyClearsBits is the rule the whole layout is built around: nothing
// is ever rewritten in place, so the device never has to refuse a program.
func TestFlashStore_AppendOnlyClearsBits(t *testing.T) {
	store, dev := newTestStore(t)
	ref := driven.LogRef("topic", 0)
	for i := 0; i < 200; i++ {
		if _, err := store.Append(ref, []byte("abc")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_, erases := dev.Counters()
	if erases != 0 {
		t.Fatalf("appending erased %d pages, it must only clear bits", erases)
	}
	if got := read(t, store, ref, 0); len(got) != 600 || strings.Trim(string(got), "abc") != "" {
		t.Fatalf("the block holds %d bytes: %q", len(got), got)
	}
}

// TestFlashStore_TruncateErasesAndRewrites: a byte cannot be unwritten, so shortening a
// page costs an erase and a rewrite, which is the one place the adapter pays for flash.
func TestFlashStore_TruncateErasesAndRewrites(t *testing.T) {
	store, dev := newTestStore(t)
	ref := driven.LogRef("topic", 0)
	if _, err := store.Append(ref, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	_, before := dev.Counters()
	if err := store.Truncate(ref, 5); err != nil {
		t.Fatal(err)
	}
	_, after := dev.Counters()
	if after != before+1 {
		t.Fatalf("truncating inside a page erased %d pages, want 1", after-before)
	}
	if got := read(t, store, ref, 0); string(got) != "hello" {
		t.Fatalf("after truncate the block holds %q", got)
	}
	if _, err := store.Append(ref, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, store, ref, 0); string(got) != "hello!" {
		t.Fatalf("after appending to the rewritten page: %q", got)
	}
}

func TestFlashStore_TopicNameMustFitThePageHeader(t *testing.T) {
	store, _ := newTestStore(t)
	tooLong := domain.TopicName(strings.Repeat("t", MaxTopicNameLength+1))
	if _, err := store.CreateTopic(tooLong); !errors.Is(err, domain.ErrInvalidTopicName) {
		t.Fatalf("create: err=%v, want ErrInvalidTopicName", err)
	}
	if _, err := store.Append(driven.LogRef(tooLong, 0), []byte("x")); !errors.Is(err, domain.ErrInvalidTopicName) {
		t.Fatalf("append: err=%v, want ErrInvalidTopicName", err)
	}
	fits := domain.TopicName(strings.Repeat("t", MaxTopicNameLength))
	if _, err := store.Append(driven.LogRef(fits, 0), []byte("x")); err != nil {
		t.Fatalf("append to the longest name that fits: %v", err)
	}
}

func TestFlashStore_IsNotSyncable(t *testing.T) {
	store, _ := newTestStore(t)
	// bytes programmed into flash are already where a power cut would leave them
	if _, isSyncable := interface{}(store).(driven.Syncable); isSyncable {
		t.Fatal("the flash store must not claim to sync")
	}
}

func TestFlashStore_RejectsPagesTooSmallForAHeader(t *testing.T) {
	if _, err := New(NewRAMDevice(4, 32)); err == nil {
		t.Fatal("a page smaller than the header was accepted")
	}
}

func TestRAMDevice_ProgramOnlyClearsBits(t *testing.T) {
	dev := NewRAMDevice(2, 64)
	if err := dev.ProgramPage(0, 0, []byte{0xf0}); err != nil {
		t.Fatal(err)
	}
	if err := dev.ProgramPage(0, 0, []byte{0xe0}); err != nil {
		t.Fatalf("clearing one more bit: %v", err)
	}
	if err := dev.ProgramPage(0, 0, []byte{0xff}); !errors.Is(err, ErrNotErased) {
		t.Fatalf("setting a bit back: err=%v, want ErrNotErased", err)
	}
	if err := dev.ErasePage(0); err != nil {
		t.Fatal(err)
	}
	if err := dev.ProgramPage(0, 0, []byte{0xff}); err != nil {
		t.Fatalf("after erasing: %v", err)
	}
	if err := dev.ProgramPage(5, 0, []byte{0}); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("programming a page that is not there: err=%v", err)
	}
}

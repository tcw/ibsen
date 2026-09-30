package topic

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// Power cuts, as opposed to the process crashes in topicAccess_crash_test.go: what was not
// synced is lost, whatever the process had written. The media is faultfs.PageCache. Each test
// here was found by the nemesis test in adapter/driver/history and is pinned on its own.

// newPowerCutTopic opens a topic on a store whose media can lose power. The topic directory is
// made durable first, so these tests are about blocks rather than about the directory.
func newPowerCutTopic(t *testing.T, params Params) (*Topic, *faultfs.PageCache, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "t"), 0744); err != nil {
		t.Fatal(err)
	}
	cache := faultfs.NewPageCache()
	params.Store = filestore.New(cache, dir)
	params.TopicName = "t"
	topic := NewLogTopic(params)
	if err := topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	return topic, cache, dir
}

// afterPowerCut cuts the power, lets loss decide what survives, and opens the topic again on
// the real filesystem, as a restarted server would.
func afterPowerCut(t *testing.T, old *Topic, cache *faultfs.PageCache, loss faultfs.Loss, dir string, params Params) (*Topic, error) {
	t.Helper()
	cache.Crash()
	old.Close()
	if err := cache.Recover(loss); err != nil {
		t.Fatal(err)
	}
	params.Store = filestore.NewOS(dir)
	params.TopicName = "t"
	recovered := NewLogTopic(params)
	t.Cleanup(recovered.Close)
	return recovered, recovered.LoadOrCreate()
}

func readPayloads(t *testing.T, topic *Topic) ([]string, error) {
	t.Helper()
	got, err := readAllFrom(topic, 0, 100)
	if errors.Is(err, domain.NoEntriesFound) {
		err = nil
	}
	var payloads []string
	for _, entry := range got {
		payloads = append(payloads, string(entry.Entry))
	}
	return payloads, err
}

// Linux marks the pages a failed fsync could not write clean, so the next fsync succeeds
// without writing them. The flusher used to treat a failed flush as retryable: the next
// batch's fsync then succeeded, and the write it acknowledged sat behind a hole that a power
// cut exposed, where recovery truncated it away. Now the failure stops the topic, so nothing
// is ever acknowledged behind the hole.
func TestPowerCut_aFailedFsyncIsNotRetryable(t *testing.T) {
	params := Params{MaxBlockSize: 1 << 20}
	topic, cache, dir := newPowerCutTopic(t, params)
	if err := writeOne(topic, "before"); err != nil {
		t.Fatal(err)
	}
	cache.FailSyncs(".log", 1)
	if err := writeOne(topic, "lost"); err == nil {
		t.Fatal("a write whose fsync failed was acknowledged")
	}
	if err := writeOne(topic, "after"); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("a write after the failed fsync returned %v, want ErrFlushFailed", err)
	}

	recovered, err := afterPowerCut(t, topic, cache, faultfs.LoseEverything(), dir, params)
	if err != nil {
		t.Fatalf("the topic does not load after the power cut: %v", err)
	}
	got, err := readPayloads(t, recovered)
	if err != nil {
		t.Fatalf("the topic cannot be read after the power cut: %v", err)
	}
	if strings.Join(got, ",") != "before" {
		t.Fatalf("the log holds %v, want only the write acknowledged before the failure", got)
	}
	// and the topic takes writes again once it has been opened afresh
	if err = writeOne(recovered, "reopened"); err != nil {
		t.Fatalf("the reopened topic refused a write: %v", err)
	}
}

// A write that rolls over to a new block used to leave the old block's tail unsynced until
// the batch was flushed, and writeback may put the new block on the media before it. Recovery
// looks only at the head block, so an old block left short was a permanent hole in the
// offsets, and one left torn was a topic nobody could read past. The old head is now synced
// before the new block exists, so a power cut during the rollover finds only the old block,
// and that is the head recovery repairs.
func TestPowerCut_aRolloverWaitsForTheOldBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		// how many unsynced bytes of the old block's tail reach the media
		oldTail func(unsynced int) int
	}{
		{"its tail is missing", func(int) int { return 0 }},
		{"its tail is torn", func(unsynced int) int { return unsynced / 2 }},
		{"its tail is whole", func(unsynced int) int { return unsynced }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			one := [][]byte{[]byte("a0")}
			frame, _, err := NewLogTopic(Params{Store: newSyncGate(), TopicName: "t", MaxBlockSize: 1}).buildFrames(&one)
			if err != nil {
				t.Fatal(err)
			}
			// a block holds two single-entry writes, the second taking it over the bound
			params := Params{MaxBlockSize: len(frame)}
			topic, cache, dir := newPowerCutTopic(t, params)
			hold := &heldSyncs{BlockStore: topic.Store, release: make(chan struct{})}
			topic.Store = hold
			topic.flush = newFlusher(hold, 0, 0)
			close(hold.release)
			if err = writeOne(topic, "a0"); err != nil {
				t.Fatal(err)
			}

			// a1 goes into the old block and waits for its flush; b2 has to roll over, and must
			// not get a block of its own until the old one is durable
			hold.hold()
			go func() { _ = writeOne(topic, "a1") }()
			eventually(t, "a1 to be appended", func() bool { return nextOffsetOf(topic) == 2 })
			rolled := make(chan error, 1)
			go func() { rolled <- writeOne(topic, "b2") }()
			// b2's rollover waits for the old block while a1's flush of it is held, and holds
			// the topic's lock meanwhile, so b2 has no block to be appended to yet
			select {
			case err := <-rolled:
				t.Fatalf("the rollover did not wait for the old block: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			loss := faultfs.KeepBytes(func(path string, unsynced int) int {
				if strings.HasSuffix(path, "00000000000000000000.log") {
					return tc.oldTail(unsynced)
				}
				return unsynced
			})
			cache.Crash()
			hold.let()
			recovered, err := afterPowerCut(t, topic, cache, loss, dir, params)
			if err != nil {
				t.Fatalf("the topic does not load after the power cut: %v", err)
			}
			got, err := readAllFrom(recovered, 0, 100)
			if err != nil && !errors.Is(err, domain.NoEntriesFound) {
				t.Fatalf("the topic cannot be read after the power cut: %v", err)
			}
			if len(got) == 0 || string(got[0].Entry) != "a0" {
				t.Fatalf("the acknowledged a0 is gone: %v", got)
			}
			for i, entry := range got {
				if entry.Offset != uint64(i) {
					t.Fatalf("the log has a hole: position %d holds offset %d", i, entry.Offset)
				}
				if string(entry.Entry) == "b2" {
					t.Fatal("b2 survived in a block that was never meant to exist yet")
				}
			}
			if err = writeOne(recovered, "next"); err != nil {
				t.Fatalf("the recovered topic refused a write: %v", err)
			}
		})
	}
}

// heldSyncs is a store whose syncs wait for the test, forwarding to the store underneath.
type heldSyncs struct {
	driven.BlockStore
	mu      sync.Mutex
	release chan struct{}
}

func (h *heldSyncs) hold() {
	h.mu.Lock()
	h.release = make(chan struct{})
	h.mu.Unlock()
}

func (h *heldSyncs) let() {
	h.mu.Lock()
	close(h.release)
	h.mu.Unlock()
}

func (h *heldSyncs) Sync(ref driven.BlockRef) error {
	h.mu.Lock()
	release := h.release
	h.mu.Unlock()
	<-release
	_, err := driven.Sync(h.BlockStore, ref)
	return err
}

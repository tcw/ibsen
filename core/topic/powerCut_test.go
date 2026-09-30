package topic

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
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

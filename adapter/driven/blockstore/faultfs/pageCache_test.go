package faultfs_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/conformance"
	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/port/driven"
)

// Each rule the model claims is pinned here against the seam directly, since everything the
// crash tests conclude rests on the model being the model it says it is.

func appendTo(t *testing.T, fs filestore.FS, path string, data string) {
	t.Helper()
	file, err := fs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func syncPath(fs filestore.FS, path string) error {
	file, err := fs.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func mustSync(t *testing.T, fs filestore.FS, path string) {
	t.Helper()
	if err := syncPath(fs, path); err != nil {
		t.Fatal(err)
	}
}

func readThrough(t *testing.T, fs filestore.FS, path string) string {
	t.Helper()
	file, err := fs.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func crashAndRecover(t *testing.T, cache *faultfs.PageCache, loss faultfs.Loss) {
	t.Helper()
	cache.Crash()
	if err := cache.Recover(loss); err != nil {
		t.Fatal(err)
	}
}

func onDisk(t *testing.T, path string) (string, bool) {
	t.Helper()
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(content), true
}

// a directory whose own name is durable, which a test's temporary directory is
func durableDir(t *testing.T) string {
	return t.TempDir()
}

func TestPageCache_syncedDataSurvivesAndUnsyncedDoesNot(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "synced")
	mustSync(t, cache, path)
	mustSync(t, cache, dir)
	appendTo(t, cache, path, "-unsynced")

	if got := readThrough(t, cache, path); got != "synced-unsynced" {
		t.Fatalf("the cache reads %q", got)
	}
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "synced" {
		t.Fatalf("after the crash the media holds %q", got)
	}
}

func TestPageCache_aNewNameNeedsItsDirectorySynced(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "data")
	// the file's data is durable, its name is not
	mustSync(t, cache, path)
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if _, there := onDisk(t, path); there {
		t.Fatal("a file whose directory was never synced survived the crash")
	}

	appendTo(t, cache, path, "data")
	mustSync(t, cache, path)
	mustSync(t, cache, dir)
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "data" {
		t.Fatalf("a synced file in a synced directory holds %q", got)
	}
}

func TestPageCache_aNewDirectoryNeedsItsParentSynced(t *testing.T) {
	root := durableDir(t)
	topic := filepath.Join(root, "topic")
	path := filepath.Join(topic, "block")
	cache := faultfs.NewPageCache()
	if err := cache.Mkdir(topic, 0744); err != nil {
		t.Fatal(err)
	}
	appendTo(t, cache, path, "data")
	mustSync(t, cache, path)
	mustSync(t, cache, topic)
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if _, err := os.Stat(topic); !os.IsNotExist(err) {
		t.Fatalf("a directory whose parent was never synced survived the crash: %v", err)
	}

	if err := cache.Mkdir(topic, 0744); err != nil {
		t.Fatal(err)
	}
	appendTo(t, cache, path, "data")
	mustSync(t, cache, path)
	mustSync(t, cache, topic)
	mustSync(t, cache, root)
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "data" {
		t.Fatalf("the block holds %q", got)
	}
}

func TestPageCache_aRemovalNeedsItsDirectorySynced(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	if err := os.WriteFile(path, []byte("durable"), 0600); err != nil {
		t.Fatal(err)
	}
	cache := faultfs.NewPageCache()
	if err := cache.Remove(path); err != nil {
		t.Fatal(err)
	}
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "durable" {
		t.Fatalf("an unsynced removal took the file with it: %q", got)
	}
}

func TestPageCache_anUnsyncedTruncateIsUndone(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	if err := os.WriteFile(path, []byte("durable"), 0600); err != nil {
		t.Fatal(err)
	}
	cache := faultfs.NewPageCache()
	file, err := cache.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(3); err != nil {
		t.Fatal(err)
	}
	file.Close()
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "durable" {
		t.Fatalf("got %q", got)
	}
}

// Linux since 4.13: a failed fsync marks the pages it failed to write clean. They stay
// readable, the next fsync succeeds without writing them, and the crash shows the hole.
func TestPageCache_aFailedSyncLosesItsDataForGood(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "one")
	mustSync(t, cache, path)
	mustSync(t, cache, dir)

	appendTo(t, cache, path, "two")
	cache.FailSyncs("block", 1)
	if err := syncPath(cache, path); !errors.Is(err, faultfs.ErrSyncFailed) {
		t.Fatalf("the sync did not fail: %v", err)
	}
	appendTo(t, cache, path, "three")
	mustSync(t, cache, path)
	if got := readThrough(t, cache, path); got != "onetwothree" {
		t.Fatalf("the cache still reads the lost bytes, and should: got %q", got)
	}

	crashAndRecover(t, cache, faultfs.LoseEverything())
	got, _ := onDisk(t, path)
	if want := "one\x00\x00\x00three"; got != want {
		t.Fatalf("after the crash the media holds %q, want %q", got, want)
	}
}

func TestPageCache_rewritingLostBytesMakesThemDirtyAgain(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "")
	mustSync(t, cache, dir)
	appendTo(t, cache, path, "lost")
	cache.FailSyncs("block", 1)
	_ = syncPath(cache, path)

	file, err := cache.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("kept")); err != nil {
		t.Fatal(err)
	}
	file.Close()
	mustSync(t, cache, path)
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if got, _ := onDisk(t, path); got != "kept" {
		t.Fatalf("got %q", got)
	}
}

func TestPageCache_losingSomeKeepsAPrefix(t *testing.T) {
	sawShorter, sawLonger := false, false
	for seed := uint64(0); seed < 50; seed++ {
		dir := durableDir(t)
		path := filepath.Join(dir, "block")
		cache := faultfs.NewPageCache()
		appendTo(t, cache, path, "base")
		mustSync(t, cache, path)
		mustSync(t, cache, dir)
		appendTo(t, cache, path, "-one")
		appendTo(t, cache, path, "-two")
		crashAndRecover(t, cache, faultfs.LoseSome(seed))
		got, _ := onDisk(t, path)
		if !bytes.HasPrefix([]byte("base-one-two"), []byte(got)) || len(got) < len("base") {
			t.Fatalf("seed %d: %q is not the synced part and a prefix of the rest", seed, got)
		}
		sawShorter = sawShorter || got == "base"
		sawLonger = sawLonger || len(got) > len("base-one")
	}
	if !sawShorter || !sawLonger {
		t.Fatal("fifty seeds never lost everything or never kept most of it")
	}
}

func TestPageCache_everyCallFailsAfterACrash(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "data")
	file, err := cache.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	cache.Crash()
	if _, err := file.Write([]byte("more")); !errors.Is(err, faultfs.ErrCrashed) {
		t.Fatalf("a write after the crash gave %v", err)
	}
	if _, err := cache.OpenFile(path, os.O_RDONLY, 0); !errors.Is(err, faultfs.ErrCrashed) {
		t.Fatalf("an open after the crash gave %v", err)
	}
	if err := file.Sync(); !errors.Is(err, faultfs.ErrCrashed) {
		t.Fatalf("a sync after the crash gave %v", err)
	}
}

// Until it crashes the model is the real filesystem: every call really happens, so a store on
// it behaves exactly as on the directory underneath. The shared suite is what says so.
func TestPageCache_storeConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driven.BlockStore {
		return filestore.New(faultfs.NewPageCache(), t.TempDir())
	})
}

func TestPageCache_theTraceIsTheCallsThatChangeTheMedia(t *testing.T) {
	dir := durableDir(t)
	cache := faultfs.NewPageCache()
	cache.CountOnly(func(path string) bool { return !strings.HasSuffix(path, ".skip") })
	appendTo(t, cache, filepath.Join(dir, "block"), "data")
	appendTo(t, cache, filepath.Join(dir, "ignored.skip"), "data")
	mustSync(t, cache, filepath.Join(dir, "block"))
	_ = readThrough(t, cache, filepath.Join(dir, "block"))

	var got []string
	for _, call := range cache.Trace() {
		got = append(got, call.Kind+" "+filepath.Base(call.Path))
	}
	if want := "create block,write block,sync block"; strings.Join(got, ",") != want {
		t.Fatalf("trace %v, want %s", got, want)
	}
}

func TestPageCache_aCrashAtACallStopsItHappening(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	cache.CrashAtCall(4) // create, write, sync, then this write
	appendTo(t, cache, path, "synced")
	mustSync(t, cache, path)
	// the directory sync is the fourth call, and the power goes out as it begins
	if err := syncPath(cache, dir); !errors.Is(err, faultfs.ErrCrashed) {
		t.Fatalf("the fourth call gave %v, want the crash", err)
	}
	if !cache.Crashed() {
		t.Fatal("the fourth call did not cut the power")
	}
	if err := cache.Recover(faultfs.LoseEverything()); err != nil {
		t.Fatal(err)
	}
	if _, there := onDisk(t, path); there {
		t.Fatal("the directory sync the power went out at happened anyway")
	}
}

func TestPageCache_aFailedDirectorySyncLeavesItsNamesUndurable(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "data")
	mustSync(t, cache, path)
	cache.FailAtCall(4)
	if err := syncPath(cache, dir); !errors.Is(err, faultfs.ErrSyncFailed) {
		t.Fatalf("the directory sync gave %v", err)
	}
	crashAndRecover(t, cache, faultfs.LoseEverything())
	if _, there := onDisk(t, path); there {
		t.Fatal("a name whose directory sync failed survived the power cut")
	}
}

func TestPageCache_durableContainsFollowsDataAndEveryName(t *testing.T) {
	root := durableDir(t)
	topic := filepath.Join(root, "topic")
	path := filepath.Join(topic, "block")
	cache := faultfs.NewPageCache()
	durable := func() bool {
		t.Helper()
		found, err := cache.DurableContains(root, []byte("needle"))
		if err != nil {
			t.Fatal(err)
		}
		return found
	}
	if err := cache.Mkdir(topic, 0744); err != nil {
		t.Fatal(err)
	}
	appendTo(t, cache, path, "hay-needle-hay")
	for _, step := range []struct {
		sync string
		want bool
	}{
		{"", false},    // in the cache only
		{path, false},  // the data is on the media, the file's name is not
		{topic, false}, // the file's name is, the topic's is not
		{root, true},   // every name on the way is
	} {
		if step.sync != "" {
			mustSync(t, cache, step.sync)
		}
		if got := durable(); got != step.want {
			t.Fatalf("after syncing %q DurableContains is %v, want %v", step.sync, got, step.want)
		}
	}

	cache.FailSyncs("block2", 1)
	appendTo(t, cache, filepath.Join(topic, "block2"), "lost-needle2")
	_ = syncPath(cache, filepath.Join(topic, "block2"))
	mustSync(t, cache, topic)
	if found, _ := cache.DurableContains(root, []byte("needle2")); found {
		t.Fatal("bytes a failed fsync dropped count as durable")
	}
}

// posix_fadvise(DONTNEED) on the model: the pages a failed fsync dropped are clean, so they are
// evicted and read back from the media, which never held them. Everything else stays.
func TestPageCache_droppingTheCacheShowsWhatAFailedFsyncLost(t *testing.T) {
	dir := durableDir(t)
	path := filepath.Join(dir, "block")
	cache := faultfs.NewPageCache()
	appendTo(t, cache, path, "one")
	mustSync(t, cache, path)
	mustSync(t, cache, dir)
	appendTo(t, cache, path, "two")
	cache.FailSyncs("block", 1)
	_ = syncPath(cache, path)
	appendTo(t, cache, path, "three")

	if err := cache.DropCache(path); err != nil {
		t.Fatal(err)
	}
	if got := readThrough(t, cache, path); got != "one\x00\x00\x00three" {
		t.Fatalf("after dropping the cache the file reads %q", got)
	}
}

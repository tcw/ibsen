package topic

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/core/domain"
)

// A read that spans two blocks must not skip the end of the first when a flush lands while it
// runs. The first block is read up to the durable offset taken when the read began; the next
// block used to be read up to the durable offset at that moment, so entries between the two
// boundaries that sat at the end of the first block were never sent, and the read carried on
// from the next block's first offset, so a tailing reader never saw them. A read now stops at
// the boundary it began with, and the next read picks up from there. Found by the nemesis
// test in adapter/driver/history.
func TestReadDoesNotSkipTheEndOfABlockWhenAFlushLandsDuringIt(t *testing.T) {
	store := newSyncGate()
	three := [][]byte{[]byte("a0"), []byte("a1"), []byte("a2")}
	frames, _, err := NewLogTopic(Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20}).buildFrames(&three)
	if err != nil {
		t.Fatal(err)
	}
	// a batch is due at three entries and otherwise waits an hour. The first write fills
	// block A exactly to the bound and is due at once; a3 takes A over the bound and waits;
	// b4 rolls over to B, which syncs A but leaves a3's batch, and so the durable offset, where
	// they were
	topic := NewLogTopic(Params{
		Store: store, TopicName: "t", MaxBlockSize: len(frames), FlushEntries: 3, FlushInterval: time.Hour,
	})
	if err = topic.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(topic.Close)
	if err = topic.Write(&three); err != nil {
		t.Fatal(err)
	}
	var writes sync.WaitGroup
	for _, payload := range []string{"a3", "b4"} {
		want := nextOffsetOf(topic) + 1
		writes.Add(1)
		go func() { defer writes.Done(); _ = writeOne(topic, payload) }()
		eventually(t, "the write to be appended", func() bool { return nextOffsetOf(topic) == want })
	}
	if durable := topic.durableOffset(); durable != 3 {
		t.Fatalf("durable offset is %d before the read, want 3", durable)
	}

	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		first := true
		for batch := range logChan {
			for _, entry := range *batch {
				got = append(got, string(entry.Entry))
			}
			if first {
				// the read has begun with offsets 0 to 2 durable; the third entry of the
				// waiting batch makes it due, and it is flushed before the read goes on
				first = false
				writes.Add(1)
				go func() { defer writes.Done(); _ = writeOne(topic, "b5") }()
				writes.Wait()
			}
			wg.Done()
		}
	}()
	err = topic.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: 0, BatchSize: 1})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	all := []string{"a0", "a1", "a2", "a3", "b4", "b5"}
	if len(got) < 3 {
		t.Fatalf("the read gave %v, less than was durable when it began", got)
	}
	for i, entry := range got {
		if entry != all[i] {
			t.Fatalf("the read gave %v: entry %d is %q, want %q", got, i, entry, all[i])
		}
	}
	// a tailing reader carries on from where the read stopped, and must find the rest there
	rest, err := readAllFrom(topic, domain.Offset(len(got)), 1)
	if err != nil && !errors.Is(err, domain.NoEntriesFound) {
		t.Fatal(err)
	}
	for _, entry := range rest {
		got = append(got, string(entry.Entry))
	}
	if strings.Join(got, ",") != strings.Join(all, ",") {
		t.Fatalf("reading on from where the read stopped gave %v, want %v", got, all)
	}
}

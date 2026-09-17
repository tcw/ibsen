package topic

import (
	"sync"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// DefaultFlushEntries is how many entries may be waiting before a flush is forced. One means
// every write is flushed before it is acknowledged, which is the safe default: raising it
// trades the latency of a write for fewer syncs.
const DefaultFlushEntries uint32 = 1

// flusher decides when appended entries reach durable media and tells a writer when its own
// entries got there. A write is acknowledged, and becomes visible to readers, only once the
// flush covering it has returned, so a reader never sees an entry that a power cut could
// take back.
//
// There is no background goroutine. The writer that needs its entries durable drives the
// flush, and writers whose entries joined the same batch wait on it. That keeps the core
// free of timers it did not start and of goroutines that outlive a topic.
//
// A store that cannot sync has nothing to push: its entries are durable the moment Append
// returns, and none of this machinery runs.
type flusher struct {
	store    driven.BlockStore
	syncable bool
	entries  uint32
	interval time.Duration

	mu      sync.Mutex
	durable domain.Offset
	pending *pendingFlush
	running bool
	// changed is closed and replaced whenever a flush finishes or a driver stops, so a
	// writer waiting on a batch it cannot drive yet knows to look again
	changed chan struct{}
}

// pendingFlush is the batch that will cover everything appended since the last flush started.
// Writers wait on done and read err once it is closed; neither is written after that.
type pendingFlush struct {
	target  domain.Offset
	blocks  map[driven.BlockRef]struct{}
	entries uint32
	opened  time.Time
	done    chan struct{}
	err     error
}

// newFlusher builds the flush policy for one topic. entries of 0 means DefaultFlushEntries,
// and an interval of 0 means a batch is never held back waiting for more entries.
func newFlusher(store driven.BlockStore, entries uint32, interval time.Duration) *flusher {
	if entries == 0 {
		entries = DefaultFlushEntries
	}
	_, syncable := store.(driven.Syncable)
	return &flusher{
		store: store, syncable: syncable, entries: entries, interval: interval,
		changed: make(chan struct{}),
	}
}

// reset declares everything below offset durable, which is what loading a topic means: what
// the recovered block holds is already on the media.
func (f *flusher) reset(offset domain.Offset) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.durable = offset
	f.pending = nil
}

// durableOffset is the read boundary: the offset after the newest entry known to be durable.
func (f *flusher) durableOffset() domain.Offset {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.durable
}

// appended records count entries written into ref, taking the log up to target, and returns
// the batch that will make them durable. It returns nil when there is nothing to wait for.
func (f *flusher) appended(ref driven.BlockRef, target domain.Offset, count int) *pendingFlush {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.syncable {
		if target > f.durable {
			f.durable = target
		}
		return nil
	}
	if f.pending == nil {
		f.pending = &pendingFlush{
			blocks: make(map[driven.BlockRef]struct{}),
			opened: time.Now(),
			done:   make(chan struct{}),
		}
	}
	f.pending.blocks[ref] = struct{}{}
	f.pending.entries += uint32(count)
	if target > f.pending.target {
		f.pending.target = target
	}
	return f.pending
}

// wait blocks until p is durable, driving the flush itself when the policy says it is due
// and no one else is driving. It returns the error of the flush that covered p.
func (f *flusher) wait(p *pendingFlush) error {
	if p == nil {
		return nil
	}
	for {
		f.mu.Lock()
		current := f.pending == p
		running := f.running
		due := current && f.dueLocked(p)
		changed := f.changed
		remaining := f.interval - time.Since(p.opened)
		f.mu.Unlock()

		switch {
		case !current:
			// a driver has taken p, and closes it whether its sync succeeds or fails
			<-p.done
			return p.err
		case due && !running:
			f.drive()
		case due:
			// due, but another driver is busy: it will either reach p or stop and say so
			select {
			case <-p.done:
				return p.err
			case <-changed:
			}
		default:
			timer := time.NewTimer(remaining)
			select {
			case <-p.done:
				timer.Stop()
				return p.err
			case <-changed:
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}

// flushRemaining pushes whatever a failed flush left behind. A writer waits for its own
// batch, so the only way unflushed entries outlive their writer is a flush that failed.
func (f *flusher) flushRemaining() {
	f.mu.Lock()
	pending, running := f.pending, f.running
	f.mu.Unlock()
	if pending == nil || running {
		return
	}
	f.drive()
}

// drive flushes batches until none is left. Only one goroutine drives at a time; the others
// wait on the batch they joined.
//
// Once driving, it takes each batch as it finds it rather than re-applying the policy: the
// next batch formed while the previous one was syncing, so it has already waited, and one
// more sync is cheaper than the round of waiting the policy would add.
func (f *flusher) drive() {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return
	}
	f.running = true
	for f.pending != nil {
		batch := f.pending
		f.pending = nil
		f.mu.Unlock()

		err := f.syncBlocks(batch)

		f.mu.Lock()
		batch.err = err
		if err == nil && batch.target > f.durable {
			f.durable = batch.target
		} else if err != nil {
			// the entries are written but their durability is unknown, so they stay in the
			// next batch and are tried again
			f.requeueLocked(batch)
		}
		close(batch.done)
		f.announceLocked()
		if err != nil {
			// a sync that just failed is not worth an immediate retry, which would spin;
			// the next write, or Close, tries again
			break
		}
	}
	f.running = false
	f.announceLocked()
	f.mu.Unlock()
}

// announceLocked tells everyone waiting that the flusher's state moved.
func (f *flusher) announceLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// requeueLocked folds a failed batch's blocks back into the next one, so a later flush covers
// them. Its target is kept too: those entries are still not known to be durable.
func (f *flusher) requeueLocked(batch *pendingFlush) {
	if f.pending == nil {
		f.pending = &pendingFlush{
			blocks: make(map[driven.BlockRef]struct{}),
			opened: batch.opened,
			done:   make(chan struct{}),
		}
	}
	for ref := range batch.blocks {
		f.pending.blocks[ref] = struct{}{}
	}
	f.pending.entries += batch.entries
	if batch.target > f.pending.target {
		f.pending.target = batch.target
	}
	if batch.opened.Before(f.pending.opened) {
		f.pending.opened = batch.opened
	}
}

// dueLocked reports whether a batch has waited long enough or grown big enough to flush. An
// interval of zero never holds a batch back, so the entry count only groups writers that
// arrive while a sync is already running.
func (f *flusher) dueLocked(p *pendingFlush) bool {
	if p.entries >= f.entries {
		return true
	}
	if f.interval <= 0 {
		return true
	}
	return time.Since(p.opened) >= f.interval
}

func (f *flusher) syncBlocks(batch *pendingFlush) error {
	var firstErr error
	for ref := range batch.blocks {
		if _, err := driven.Sync(f.store, ref); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

package topic

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// DefaultFlushEntries is how many entries may be waiting before a flush is forced. One means
// every write is flushed before it is acknowledged, which is the safe default: raising it
// trades the latency of a write for fewer syncs.
const DefaultFlushEntries uint32 = 1

// ErrFlushFailed is what every write to a topic gets once a flush of it has failed. A failed
// fsync is not retryable: Linux marks the pages it could not write clean, so the next fsync
// succeeds without writing them, and a write acknowledged after it would sit behind a hole a
// power cut exposes. The topic takes no more writes until it is opened again, which in a
// server means a restart — PostgreSQL's answer to the same fsync, for the same reason.
var ErrFlushFailed = errors.New("a flush of this topic failed; it takes no more writes until it is opened again")

// FlushMode says when a write is acknowledged. In both modes an entry becomes readable only
// once the flush covering it has returned, so a reader never sees what a power cut could take
// back; the modes differ only in what the writer is told, and when.
type FlushMode int

const (
	// FlushStrict acknowledges a write once the flush covering it has returned: nothing
	// acknowledged is lost to a power cut. It is the zero value, so a topic built by a caller
	// that says nothing is as safe as it can be.
	FlushStrict FlushMode = iota
	// FlushLenient acknowledges a write once it is appended — in the page cache, for a file —
	// and leaves the flush to whoever calls Flush: in a server, a ticker in the composition
	// root. A process crash loses nothing; a power cut loses what was acknowledged since the
	// last flush. Readers are still held to the durable offset.
	FlushLenient
)

func (m FlushMode) String() string {
	if m == FlushLenient {
		return "lenient"
	}
	return "strict"
}

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
	mode     FlushMode
	// log and topic say which entries a failed lenient flush took with it, since their
	// writers were told they were written and no longer wait to be told otherwise
	log   driven.Logger
	topic string

	mu      sync.Mutex
	durable domain.Offset
	pending *pendingFlush
	running bool
	// failed is set by the first flush that fails, and never cleared: see ErrFlushFailed
	failed error
	// syncing is held across every sync and the recording of its outcome, so syncs and
	// failures have one order: a sync that returns after another has failed cannot be taken
	// as proof of anything, and with this it cannot happen. Taken before mu, never under it.
	syncing sync.Mutex
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
func newFlusher(store driven.BlockStore, entries uint32, interval time.Duration, mode FlushMode, log driven.Logger, topic string) *flusher {
	if entries == 0 {
		entries = DefaultFlushEntries
	}
	if log == nil {
		log = driven.NopLogger{}
	}
	_, syncable := store.(driven.Syncable)
	return &flusher{
		store: store, syncable: syncable, entries: entries, interval: interval,
		mode: mode, log: log, topic: topic,
		changed: make(chan struct{}),
	}
}

// acknowledge is what a writer waits for before it is told its entries are written. In
// strict mode that is the flush covering them. In lenient mode it is nothing, unless the
// batch has already failed: entries appended behind a flush that failed while they were
// being appended must not be acknowledged, since no later flush can ever cover them.
func (f *flusher) acknowledge(p *pendingFlush) error {
	if f.mode == FlushStrict {
		return f.wait(p)
	}
	if p == nil {
		return nil
	}
	select {
	case <-p.done:
		return p.err
	default:
		return nil
	}
}

// flushAll makes everything appended before it was called durable, whatever the policy says,
// and returns the error of the flush that covered it — or the failure that stopped the topic
// earlier. It is how lenient mode is ever flushed, and how Close makes sure a clean exit
// loses nothing in either mode.
func (f *flusher) flushAll() error {
	f.mu.Lock()
	if f.failed != nil {
		err := f.failed
		f.mu.Unlock()
		return err
	}
	p := f.pending
	f.mu.Unlock()
	if p != nil {
		return f.waitFor(p, true)
	}
	// nothing new, but a driver may hold a batch it took before we were called: what was
	// appended before this call is only durable once that batch is
	for {
		f.mu.Lock()
		running := f.running
		changed := f.changed
		failed := f.failed
		f.mu.Unlock()
		if failed != nil {
			return failed
		}
		if !running {
			return nil
		}
		<-changed
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

// failure is the error a flush of this topic failed with, or nil.
func (f *flusher) failure() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed
}

// barrier makes ref durable now, outside any batch, for a caller that must not go on until
// it is: rolling over to a new block, which writeback could otherwise put on the media ahead
// of the old block's unsynced tail. A failure is a failed flush like any other and stops the
// topic. It does not move the durable offset; the batches covering ref still do that.
func (f *flusher) barrier(ref driven.BlockRef) error {
	if !f.syncable {
		return nil
	}
	f.syncing.Lock()
	defer f.syncing.Unlock()
	if err := f.failure(); err != nil {
		return err
	}
	if _, err := driven.Sync(f.store, ref); err != nil {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.failLocked(err)
		return f.failed
	}
	return nil
}

// appended records count entries written into ref, taking the log up to target, and returns
// the batch that will make them durable. It returns nil when there is nothing to wait for.
func (f *flusher) appended(ref driven.BlockRef, target domain.Offset, count int) *pendingFlush {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed != nil {
		// a flush failed after the caller checked: these entries are behind the hole too
		failed := &pendingFlush{done: make(chan struct{}), err: f.failed}
		close(failed.done)
		return failed
	}
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
	return f.waitFor(p, false)
}

// waitFor is wait, with force making the batch due at once whatever the policy says.
func (f *flusher) waitFor(p *pendingFlush, force bool) error {
	if p == nil {
		return nil
	}
	for {
		f.mu.Lock()
		current := f.pending == p
		running := f.running
		due := current && (force || f.dueLocked(p))
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

		f.syncing.Lock()
		// a barrier may have failed since the batch was taken; syncing after it would
		// succeed without writing what it dropped, so the batch fails with it
		failedBefore := f.failure()
		var err error
		if failedBefore == nil {
			err = f.syncBlocks(batch)
		}

		f.mu.Lock()
		f.syncing.Unlock()
		if failedBefore != nil {
			batch.err = failedBefore
			close(batch.done)
			f.announceLocked()
			break
		}
		if err != nil {
			if f.mode == FlushLenient {
				// their writers were told these were written, and nobody is waiting to be
				// told otherwise: the log is the only place left to say so
				f.log.Log(driven.LevelError, "a flush failed: acknowledged entries may be lost to a power cut, and the topic takes no more writes",
					driven.Str("topic", f.topic), driven.Uint64("fromOffset", uint64(f.durable)),
					driven.Uint64("toOffset", uint64(batch.target)), driven.Err(err))
			}
			batch.err = fmt.Errorf("%w: %w", ErrFlushFailed, err)
			close(batch.done)
			f.failLocked(err)
			break
		}
		if batch.target > f.durable {
			f.durable = batch.target
		}
		close(batch.done)
		f.announceLocked()
	}
	f.running = false
	f.announceLocked()
	f.mu.Unlock()
}

// failLocked stops the topic after a failed sync. What was being synced is lost to the media,
// and so is every batch behind it: no later sync can make them durable, so none of them may
// ever be acknowledged or read. Only the first failure is kept.
func (f *flusher) failLocked(err error) {
	if f.failed == nil {
		f.failed = fmt.Errorf("%w: %w", ErrFlushFailed, err)
	}
	if f.pending != nil {
		f.pending.err = f.failed
		close(f.pending.done)
		f.pending = nil
	}
	f.announceLocked()
}

// announceLocked tells everyone waiting that the flusher's state moved.
func (f *flusher) announceLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
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

// syncBlocks syncs every block of a batch, oldest first, and returns the first error. The
// order is fixed so that the same writes make the same syncs, which is what lets a test aim a
// crash at one of them by its place in the trace.
func (f *flusher) syncBlocks(batch *pendingFlush) error {
	refs := make([]driven.BlockRef, 0, len(batch.blocks))
	for ref := range batch.blocks {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Topic != refs[j].Topic {
			return refs[i].Topic < refs[j].Topic
		}
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].Block < refs[j].Block
	})
	var firstErr error
	for _, ref := range refs {
		if _, err := driven.Sync(f.store, ref); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

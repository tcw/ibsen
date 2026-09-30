package history_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/faultfs"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driver/history"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
)

// The crash at every storage call, in the manner of Molly's lineage-driven fault injection,
// as far as it fits a go test.
//
// Molly asks two things of a run. What did each good outcome rest on — its lineage — and does
// a fault that takes that support away break it? Here the first is asked at the moment of
// every promise: when a write is acknowledged, or an entry is handed to a reader, the model is
// asked whether those bytes are on the media under names that are durable all the way to the
// root. A promise without that support is a bug even if no crash ever lands on it, and this
// finds it on the first run that makes the promise, deterministically.
//
// The second is answered by brute force rather than by pruning, because the space is small
// enough to afford it: one deterministic workload is traced once without faults, and then
// run again once for every call in the trace with the power cut as that call begins, under
// each loss the model knows, and once for every sync in it with that sync failing and the
// power cut at the end. Each run recovers, writes on in a new life, and hands the whole
// history to the checker. The workload restarts halfway, so a failed sync before the restart
// is met by a process that did not see it fail.

// crashLosses are the ways a crash can go: a power cut that keeps nothing unsynced, a process
// crash that keeps everything the kernel was given, and in between.
var crashLosses = []struct {
	name string
	loss func(point int) faultfs.Loss
}{
	{"power cut", func(int) faultfs.Loss { return faultfs.LoseEverything() }},
	{"process crash", func(int) faultfs.Loss {
		return faultfs.KeepBytes(func(_ string, unsynced int) int { return unsynced })
	}},
	{"power cut keeping some", func(point int) faultfs.Loss { return faultfs.LoseSome(uint64(point)) }},
	// the loss aimed at a rollover's support: writeback put the newest block of each topic on
	// the media and not the unsynced tails of the blocks before it
	{"writeback newest first", func(int) faultfs.Loss { return faultfs.KeepBytes(keepOnlyNewestBlock) }},
}

// keepOnlyNewestBlock keeps what was not synced of the newest log block in each directory,
// and of nothing else.
func keepOnlyNewestBlock(path string, unsynced int) int {
	if !strings.HasSuffix(path, ".log") {
		return 0
	}
	newest := ""
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, entry := range entries {
		if name := entry.Name(); strings.HasSuffix(name, ".log") && name > newest {
			newest = name
		}
	}
	if filepath.Base(path) == newest {
		return unsynced
	}
	return 0
}

// step is one thing the workload does: a write, a read of a topic from the start, a restart
// of the process, which drops everything the store and the manager held in memory, or a pair
// of writes to one topic where the second arrives while the first is still being flushed.
type step struct {
	restart bool
	read    bool
	topic   domain.TopicName
	entries []string
	// second, when set, makes this a pair: entries is written first and held at its flush,
	// then second, which is large enough to roll the block over now and then
	second []string
}

// crashWorkload is the same every time it is built: two topics, blocks and frames small
// enough that a write spans frames and the log rolls over, a restart in the middle, and reads
// between the writes.
func crashWorkload(phase string, writes int, seed uint64) []step {
	rng := rand.New(rand.NewPCG(seed, 7))
	var steps []step
	for w := 0; w < writes; w++ {
		topic := domain.TopicName(fmt.Sprintf("topic-%d", rng.IntN(2)))
		entries := make([]string, 1+rng.IntN(4))
		for i := range entries {
			entries[i] = fmt.Sprintf("<%s-%d-%d>", phase, w, i)
		}
		s := step{topic: topic, entries: entries}
		if w%4 == 1 {
			s.second = make([]string, 3+rng.IntN(3))
			for i := range s.second {
				s.second[i] = fmt.Sprintf("<%s-%d-p%d>", phase, w, i)
			}
		}
		steps = append(steps, s)
		if w%3 == 2 {
			steps = append(steps, step{read: true, topic: topic})
		}
		if phase == "A" && w == writes/2 {
			steps = append(steps, step{restart: true})
		}
	}
	return steps
}

// crashRun is one run of the workload on one directory.
type crashRun struct {
	t     *testing.T
	dir   string
	cache *faultfs.PageCache
	store *gatedStore
	h     *history.History
	m     *manager.LogTopicsManager
	// unsupported is every promise made without the bytes behind it being durable
	unsupported []string
	// restartedAt is how many calls the trace held when the workload restarted the process
	restartedAt int
}

func newCrashRun(t *testing.T) *crashRun {
	cache := faultfs.NewPageCache()
	// the index is written by a goroutine on its own schedule, so counting its calls would
	// make the trace differ from run to run; it is derived from the log and never synced, so
	// no promise rests on it
	cache.CountOnly(func(path string) bool { return !strings.HasSuffix(path, ".idx") })
	return &crashRun{t: t, dir: t.TempDir(), cache: cache, h: &history.History{}}
}

// open starts a process: a new store and manager, so nothing is carried over in memory.
func (r *crashRun) open() {
	r.store = newGatedStore(filestore.New(r.cache, r.dir))
	m, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store:           r.store,
		MaxBlockSize:    300,
		MaxFrameEntries: 3,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.m = &m
}

func (r *crashRun) close() {
	if r.m != nil {
		r.m.Close()
		r.m = nil
	}
}

// supported checks that a promise rests on durable bytes: every entry acknowledged or read
// has to be on the media under durable names at the moment it is promised.
func (r *crashRun) supported(what string, entries []string) {
	for _, entry := range entries {
		durable, err := r.cache.DurableContains(r.dir, []byte(entry))
		if err != nil {
			r.t.Fatal(err)
		}
		if !durable {
			r.unsupported = append(r.unsupported, fmt.Sprintf("%s %s at call %d", what, entry, len(r.cache.Trace())))
		}
	}
}

// play runs steps until they are done or, when stopAtCrash, until the power has gone out.
func (r *crashRun) play(steps []step, stopAtCrash bool) {
	for _, s := range steps {
		if stopAtCrash && r.cache.Crashed() {
			return
		}
		switch {
		case s.restart:
			r.close()
			r.restartedAt = len(r.cache.Trace())
			r.open()
		case s.second != nil:
			r.pair(s)
		case s.read:
			seen, err := r.h.Read(r.m, 0, s.topic, 0)
			if err == nil {
				var data []string
				for _, entry := range seen {
					data = append(data, entry.Data)
				}
				r.supported("read", data)
			}
		default:
			if err := r.h.Write(r.m, 0, s.topic, s.entries); err == nil {
				r.supported("acknowledged", s.entries)
			}
		}
	}
}

// pair writes s.entries and holds it at its flush, then writes s.second beside it, then lets
// both go. With the flush held, a second write that rolls over finds the first write's tail
// unsynced: that is the state a rollover has to be safe in, and a lone writer never reaches
// it, since each of its writes is durable before the next begins.
func (r *crashRun) pair(s step) {
	r.store.holdAfterNextAppend()
	first := make(chan error, 1)
	go func() { first <- r.h.Write(r.m, 0, s.topic, s.entries) }()
	var err1, err2 error
	firstDone := false
	select {
	case <-r.store.held:
	case err1 = <-first:
		firstDone = true
	}
	second := make(chan error, 1)
	go func() { second <- r.h.Write(r.m, 1, s.topic, s.second) }()
	secondDone := false
	// the second write either appends beside the held flush or waits for the old block, which
	// cannot be seen from here; either way it is let go once it had its chance
	select {
	case <-r.store.appended:
	case err2 = <-second:
		secondDone = true
	case <-time.After(5 * time.Millisecond):
	}
	r.store.let()
	if !firstDone {
		err1 = <-first
	}
	if !secondDone {
		err2 = <-second
	}
	if err1 == nil {
		r.supported("acknowledged", s.entries)
	}
	if err2 == nil {
		r.supported("acknowledged", s.second)
	}
}

// gatedStore holds syncs for a pair: armed, it starts holding every sync once the next log
// append has completed, so the first write of a pair is held at its flush and not at a
// rollover of its own before it.
type gatedStore struct {
	*filestore.Store
	mu       sync.Mutex
	armed    bool
	gate     chan struct{}
	held     chan struct{}
	appended chan struct{}
}

func newGatedStore(store *filestore.Store) *gatedStore {
	return &gatedStore{Store: store, held: make(chan struct{}, 1), appended: make(chan struct{}, 1)}
}

func (g *gatedStore) holdAfterNextAppend() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = true
	g.gate = nil
	drain(g.held)
	drain(g.appended)
}

func (g *gatedStore) let() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gate != nil {
		close(g.gate)
	}
	g.armed, g.gate = false, nil
}

func (g *gatedStore) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	block, err := g.Store.Append(ref, data)
	if err == nil && ref.Kind == driven.Log {
		g.mu.Lock()
		if g.armed && g.gate == nil {
			g.gate = make(chan struct{})
		} else {
			signal(g.appended)
		}
		g.mu.Unlock()
	}
	return block, err
}

func (g *gatedStore) Sync(ref driven.BlockRef) error {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	if gate != nil {
		signal(g.held)
		<-gate
	}
	return g.Store.Sync(ref)
}

func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func drain(c chan struct{}) {
	select {
	case <-c:
	default:
	}
}

// check reads the directory back as a restarted server would and hands everything to the
// checker, and returns what it found wrong.
func (r *crashRun) check() []string {
	var problems []string
	final, err := readBack(r.t, r.dir)
	if err != nil {
		return append(problems, fmt.Sprintf("the log could not be read back: %v", err))
	}
	for _, anomaly := range history.Check(r.h.Ops(), final) {
		problems = append(problems, anomaly.String())
	}
	return append(problems, r.unsupported...)
}

// traceOf is a trace with the run's directory taken off every path, so two runs compare.
func traceOf(r *crashRun) []string {
	var calls []string
	for _, call := range r.cache.Trace() {
		calls = append(calls, call.Kind+" "+strings.TrimPrefix(call.Path, r.dir))
	}
	return calls
}

func TestCrashAtEveryStorageCall(t *testing.T) {
	before := crashWorkload("A", 16, 1)
	after := crashWorkload("C", 4, 2)

	// the fault-free run: its promises checked for support, its history for anomalies, and its
	// trace the list of places the power can go out
	baseline := newCrashRun(t)
	baseline.open()
	baseline.play(before, false)
	baseline.close()
	if problems := baseline.check(); len(problems) > 0 {
		t.Fatalf("the run without faults is already wrong: %v", problems)
	}
	trace := traceOf(baseline)
	again := newCrashRun(t)
	again.open()
	again.play(before, false)
	again.close()
	if got := traceOf(again); strings.Join(got, "\n") != strings.Join(trace, "\n") {
		t.Fatalf("the workload's trace differs between two runs without faults, so a call number "+
			"does not name one call:\n%v\n%v", trace, got)
	}
	t.Logf("%d storage calls, the restart after call %d", len(trace), baseline.restartedAt)
	t.Logf("%d crash runs, %d failed-sync runs", len(trace)*len(crashLosses),
		strings.Count(strings.Join(trace, "\n"), "sync "))

	failures := 0
	report := func(what string, problems []string) {
		if len(problems) == 0 {
			return
		}
		failures++
		if failures <= 20 {
			t.Errorf("%s: %d problems, first: %s", what, len(problems), problems[0])
		}
	}

	for point := 1; point <= len(trace); point++ {
		for _, loss := range crashLosses {
			what := fmt.Sprintf("%s at call %d (%s)", loss.name, point, trace[point-1])
			r := newCrashRun(t)
			r.cache.CrashAtCall(point)
			r.open()
			r.play(before, true)
			r.close()
			if !r.cache.Crashed() {
				t.Fatalf("%s: the run finished without reaching the call", what)
			}
			if got := traceOf(r); got[point-1] != trace[point-1] {
				t.Fatalf("%s: the run reached %q there instead", what, got[point-1])
			}
			if err := r.cache.Recover(loss.loss(point)); err != nil {
				t.Fatal(err)
			}
			r.open()
			r.play(after, false)
			r.close()
			report(what, r.check())
		}
	}

	for point, call := range trace {
		if !strings.HasPrefix(call, "sync ") {
			continue
		}
		what := fmt.Sprintf("failed %s at call %d, then a power cut", call, point+1)
		r := newCrashRun(t)
		r.cache.FailAtCall(point + 1)
		r.open()
		r.play(before, false)
		r.close()
		r.cache.Crash()
		if err := r.cache.Recover(faultfs.LoseEverything()); err != nil {
			t.Fatal(err)
		}
		r.open()
		r.play(after, false)
		r.close()
		report(what, r.check())
	}
	if failures > 20 {
		t.Errorf("and %d more", failures-20)
	}
}

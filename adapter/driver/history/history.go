// Package history records what concurrent clients did to a log and checks it against what the
// log holds afterwards, in the manner of Jepsen: every operation is an invocation and a
// completion on a logical clock, and the checker decides whether one log could have produced
// what every client saw.
//
// It is test support. It drives the log only through driver.LogManager, so the same history
// can be taken against the manager, the embedded root or a server behind a client, and it
// imports nothing outside the standard library and the core.
//
// A log is much easier to check than a register. Offsets are assigned by the log and there is
// one order, so there is nothing to search for: the log read back after the run is the order,
// and every rule below is a scan of it.
//
//   - Nothing is invented: every entry in the log was written by some client, and at most once.
//   - Offsets are contiguous from zero, and each is where the log put it.
//   - An acknowledged write is in the log, whole and in order.
//   - A write that failed is indeterminate, not failed: Ibsen does not promise that a write
//     returning an error left nothing behind, only that it left a prefix of itself. Since a
//     write is appended as frames and recovery truncates at the first damaged one, that is
//     exactly what survives a crash.
//   - Real time: a write acknowledged before another was invoked is before it in the log.
//   - A read saw what the log holds: every entry a reader was given is in the log at the offset
//     it was given at. This is the durability promise from the reader's side, since the log is
//     read back after the crash and a reader must never have seen what a crash took back.
package history

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
)

// Kind is what an operation did.
type Kind int

const (
	Write Kind = iota
	Read
)

// Entry is one entry as a reader saw it or as the log holds it.
type Entry struct {
	Offset uint64
	Data   string
}

// Op is one operation by one client: when it was invoked and completed on the history's
// logical clock, what it asked for and what it got.
type Op struct {
	Kind     Kind
	Process  int
	Topic    domain.TopicName
	Invoke   int64
	Complete int64
	// Written is what a write sent, one string per entry
	Written []string
	// From and Seen are what a read asked for and was given
	From uint64
	Seen []Entry
	Err  error
}

// History is a record of operations, safe for concurrent clients.
type History struct {
	clock atomic.Int64
	mu    sync.Mutex
	ops   []Op
}

func (h *History) record(op Op) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// Ops returns what has been recorded so far.
func (h *History) Ops() []Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Op(nil), h.ops...)
}

// Write writes entries through log and records the write. Entries must be unique across the
// whole history, since the log gives back no offsets and an entry is recognised by its bytes.
func (h *History) Write(log driver.LogManager, process int, topic domain.TopicName, entries []string) error {
	batch := make([][]byte, len(entries))
	for i, entry := range entries {
		batch[i] = []byte(entry)
	}
	op := Op{Kind: Write, Process: process, Topic: topic, Written: entries}
	op.Invoke = h.clock.Add(1)
	op.Err = log.Write(topic, &batch)
	op.Complete = h.clock.Add(1)
	h.record(op)
	return op.Err
}

// Read reads the topic from an offset to the end through log and records what it was given.
// A topic with nothing at or after the offset is an empty read, not an error.
func (h *History) Read(log driver.LogManager, process int, topic domain.TopicName, from uint64) ([]Entry, error) {
	op := Op{Kind: Read, Process: process, Topic: topic, From: from}
	op.Invoke = h.clock.Add(1)
	op.Seen, op.Err = ReadAll(log, topic, from)
	op.Complete = h.clock.Add(1)
	h.record(op)
	return op.Seen, op.Err
}

// ReadAll reads a topic from an offset to the end of the log, copying every entry out of the
// buffers the log hands over.
func ReadAll(log driver.LogManager, topic domain.TopicName, from uint64) ([]Entry, error) {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	var seen []Entry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range logChan {
			for _, entry := range *batch {
				seen = append(seen, Entry{Offset: entry.Offset, Data: string(entry.Entry)})
			}
			wg.Done()
		}
	}()
	err := log.Read(driver.ReadParams{
		TopicName: topic, LogChan: logChan, Wg: &wg, From: domain.Offset(from), BatchSize: 100,
	})
	wg.Wait()
	close(logChan)
	<-done
	if errors.Is(err, domain.NoEntriesFound) {
		err = nil
	}
	return seen, err
}

// Anomaly is one thing the history says that no single log could have produced.
type Anomaly struct {
	Kind   string
	Topic  domain.TopicName
	Detail string
}

func (a Anomaly) String() string {
	return fmt.Sprintf("%s in %s: %s", a.Kind, a.Topic, a.Detail)
}

// The kinds of anomaly Check reports.
const (
	BadOffsets        = "offsets not contiguous"
	Phantom           = "entry nobody wrote"
	Duplicate         = "entry written once, in the log twice"
	LostWrite         = "acknowledged write lost"
	TornWrite         = "write not whole, or not in order"
	RealTime          = "write before an earlier acknowledged write"
	ReadNotInLog      = "read saw what the log does not hold"
	ReadNotContiguous = "read not contiguous from where it asked"
)

// placed is where the log put one write: the positions of the entries of it that are there.
type placed struct {
	op        *Op
	positions []int
}

// Check compares a history with the log read back afterwards, topic by topic, and returns
// every anomaly it finds; none means one log explains everything every client saw.
func Check(ops []Op, final map[domain.TopicName][]Entry) []Anomaly {
	var anomalies []Anomaly
	byTopic := map[domain.TopicName][]*Op{}
	for i := range ops {
		byTopic[ops[i].Topic] = append(byTopic[ops[i].Topic], &ops[i])
	}
	topics := map[domain.TopicName]bool{}
	for topic := range byTopic {
		topics[topic] = true
	}
	for topic := range final {
		topics[topic] = true
	}
	for topic := range topics {
		anomalies = append(anomalies, checkTopic(topic, byTopic[topic], final[topic])...)
	}
	sort.Slice(anomalies, func(i, j int) bool { return anomalies[i].String() < anomalies[j].String() })
	return anomalies
}

func checkTopic(topic domain.TopicName, ops []*Op, log []Entry) []Anomaly {
	var anomalies []Anomaly
	report := func(kind, format string, args ...any) {
		anomalies = append(anomalies, Anomaly{Kind: kind, Topic: topic, Detail: fmt.Sprintf(format, args...)})
	}

	for i, entry := range log {
		if entry.Offset != uint64(i) {
			report(BadOffsets, "position %d holds offset %d", i, entry.Offset)
			break
		}
	}

	// who wrote what: every entry belongs to one write, at one index in it
	type origin struct {
		op    *Op
		index int
	}
	writer := map[string]origin{}
	for _, op := range ops {
		if op.Kind != Write {
			continue
		}
		for i, entry := range op.Written {
			writer[entry] = origin{op, i}
		}
	}

	positions := map[*Op][]int{}
	firstSeen := map[string]int{}
	for position, entry := range log {
		from, ok := writer[entry.Data]
		if !ok {
			report(Phantom, "offset %d holds %q", entry.Offset, entry.Data)
			continue
		}
		if earlier, twice := firstSeen[entry.Data]; twice {
			report(Duplicate, "%q at offsets %d and %d", entry.Data, earlier, position)
			continue
		}
		firstSeen[entry.Data] = position
		positions[from.op] = append(positions[from.op], position)
	}

	var writes []placed
	for _, op := range ops {
		if op.Kind != Write {
			continue
		}
		found := positions[op]
		// what is there of a write must be a prefix of it, contiguous and in order; all of it
		// if the write was acknowledged
		if len(found) > 0 {
			whole := true
			for i, position := range found {
				if position != found[0]+i || log[position].Data != op.Written[i] {
					whole = false
				}
			}
			if !whole {
				report(TornWrite, "write by process %d at %d holds its entries at positions %v", op.Process, op.Invoke, found)
			}
		}
		if op.Err == nil && len(found) != len(op.Written) {
			report(LostWrite, "write by process %d, acknowledged at %d, has %d of its %d entries in the log",
				op.Process, op.Complete, len(found), len(op.Written))
		}
		if len(found) > 0 {
			writes = append(writes, placed{op: op, positions: found})
		}
	}
	anomalies = append(anomalies, checkRealTime(topic, writes)...)

	for _, op := range ops {
		if op.Kind != Read {
			continue
		}
		for i, entry := range op.Seen {
			if entry.Offset != op.From+uint64(i) {
				report(ReadNotContiguous, "read by process %d from %d was given offset %d at position %d",
					op.Process, op.From, entry.Offset, i)
				break
			}
			if entry.Offset >= uint64(len(log)) || log[entry.Offset].Data != entry.Data {
				report(ReadNotInLog, "read by process %d at %d saw %q at offset %d",
					op.Process, op.Invoke, entry.Data, entry.Offset)
				break
			}
		}
	}
	return anomalies
}

// checkRealTime finds a write that is in the log before a write acknowledged before it was
// invoked. Sweeping the writes in the order they were invoked, while folding in the writes
// acknowledged before each, keeps it to a sort.
func checkRealTime(topic domain.TopicName, writes []placed) []Anomaly {
	var acknowledged []placed
	for _, write := range writes {
		if write.op.Err == nil {
			acknowledged = append(acknowledged, write)
		}
	}
	sort.Slice(acknowledged, func(i, j int) bool { return acknowledged[i].op.Complete < acknowledged[j].op.Complete })
	sort.Slice(writes, func(i, j int) bool { return writes[i].op.Invoke < writes[j].op.Invoke })

	var anomalies []Anomaly
	latest, latestPosition, next := (*Op)(nil), -1, 0
	for _, write := range writes {
		for next < len(acknowledged) && acknowledged[next].op.Complete < write.op.Invoke {
			last := acknowledged[next].positions[len(acknowledged[next].positions)-1]
			if last > latestPosition {
				latest, latestPosition = acknowledged[next].op, last
			}
			next++
		}
		if first := write.positions[0]; latest != nil && first < latestPosition {
			anomalies = append(anomalies, Anomaly{Kind: RealTime, Topic: topic, Detail: fmt.Sprintf(
				"write by process %d invoked at %d is at position %d, before position %d of a write acknowledged at %d",
				write.op.Process, write.op.Invoke, first, latestPosition, latest.Complete)})
		}
	}
	return anomalies
}

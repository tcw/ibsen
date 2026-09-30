package history

import (
	"errors"
	"strings"
	"testing"

	"github.com/tcw/ibsen/core/domain"
)

// A checker that never finds anything proves nothing, so every anomaly it knows is shown to
// it here in a history built by hand, next to the valid histories it has to accept.

var errIndeterminate = errors.New("the write may or may not have happened")

func write(process int, invoke, complete int64, err error, entries ...string) Op {
	return Op{Kind: Write, Process: process, Topic: "t", Invoke: invoke, Complete: complete, Written: entries, Err: err}
}

func read(process int, invoke, complete int64, from uint64, seen ...string) Op {
	op := Op{Kind: Read, Process: process, Topic: "t", Invoke: invoke, Complete: complete, From: from}
	for i, data := range seen {
		op.Seen = append(op.Seen, Entry{Offset: from + uint64(i), Data: data})
	}
	return op
}

func logOf(entries ...string) map[domain.TopicName][]Entry {
	log := make([]Entry, len(entries))
	for i, data := range entries {
		log[i] = Entry{Offset: uint64(i), Data: data}
	}
	return map[domain.TopicName][]Entry{"t": log}
}

func TestCheck_acceptsWhatOneLogExplains(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []Op
		log  map[domain.TopicName][]Entry
	}{
		{"nothing", nil, logOf()},
		{"sequential writes",
			[]Op{write(1, 1, 2, nil, "a", "b"), write(1, 3, 4, nil, "c")},
			logOf("a", "b", "c")},
		{"concurrent writes in either order",
			[]Op{write(1, 1, 4, nil, "a"), write(2, 2, 3, nil, "b")},
			logOf("b", "a")},
		{"a failed write that left nothing",
			[]Op{write(1, 1, 2, errIndeterminate, "a", "b"), write(1, 3, 4, nil, "c")},
			logOf("c")},
		{"a failed write that left a prefix of itself",
			[]Op{write(1, 1, 2, errIndeterminate, "a", "b", "c")},
			logOf("a", "b")},
		{"a failed write that left all of itself",
			[]Op{write(1, 1, 2, errIndeterminate, "a", "b")},
			logOf("a", "b")},
		{"reads of what is there",
			[]Op{write(1, 1, 2, nil, "a", "b"), read(2, 3, 4, 1, "b"), read(2, 5, 6, 0, "a", "b")},
			logOf("a", "b")},
		{"a read that saw nothing yet",
			[]Op{read(2, 1, 2, 0), write(1, 3, 4, nil, "a")},
			logOf("a")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if anomalies := Check(tc.ops, tc.log); len(anomalies) != 0 {
				t.Fatalf("a valid history was rejected: %v", anomalies)
			}
		})
	}
}

func TestCheck_findsWhatNoLogExplains(t *testing.T) {
	for _, tc := range []struct {
		want string
		ops  []Op
		log  map[domain.TopicName][]Entry
	}{
		{LostWrite,
			[]Op{write(1, 1, 2, nil, "a"), write(1, 3, 4, nil, "b")},
			logOf("a")},
		{LostWrite, // half of an acknowledged write is as lost as all of it
			[]Op{write(1, 1, 2, nil, "a", "b")},
			logOf("a")},
		{TornWrite, // a failed write may leave a prefix, not a suffix
			[]Op{write(1, 1, 2, errIndeterminate, "a", "b")},
			logOf("b")},
		{TornWrite, // nor be split around another write
			[]Op{write(1, 1, 4, nil, "a", "b"), write(2, 2, 3, nil, "c")},
			logOf("a", "c", "b")},
		{Phantom,
			[]Op{write(1, 1, 2, nil, "a")},
			logOf("a", "z")},
		{Duplicate,
			[]Op{write(1, 1, 2, nil, "a")},
			logOf("a", "a")},
		{RealTime,
			[]Op{write(1, 1, 2, nil, "a"), write(2, 3, 4, nil, "b")},
			logOf("b", "a")},
		{RealTime, // an indeterminate write is still bound by what was acknowledged before it began
			[]Op{write(1, 1, 2, nil, "a"), write(2, 3, 4, errIndeterminate, "b")},
			logOf("b", "a")},
		{ReadNotInLog, // the durability promise: a reader saw what a crash then took back
			[]Op{write(1, 1, 2, errIndeterminate, "a"), read(2, 3, 4, 0, "a")},
			logOf()},
		{ReadNotInLog,
			[]Op{write(1, 1, 2, nil, "a", "b"), read(2, 3, 4, 0, "b")},
			logOf("a", "b")},
		{BadOffsets,
			[]Op{write(1, 1, 2, nil, "a", "b")},
			map[domain.TopicName][]Entry{"t": {{Offset: 0, Data: "a"}, {Offset: 2, Data: "b"}}}},
	} {
		t.Run(tc.want, func(t *testing.T) {
			anomalies := Check(tc.ops, tc.log)
			for _, anomaly := range anomalies {
				if anomaly.Kind == tc.want {
					return
				}
			}
			t.Fatalf("want %q, got %v", tc.want, anomalies)
		})
	}
}

func TestCheck_readsTopicsApart(t *testing.T) {
	ops := []Op{write(1, 1, 2, nil, "a")}
	ops[0].Topic = "other"
	anomalies := Check(ops, logOf("a"))
	var kinds []string
	for _, anomaly := range anomalies {
		kinds = append(kinds, anomaly.Kind)
	}
	// the write went to one topic and the entry is in another: lost there, invented here
	if got := strings.Join(kinds, ","); got != LostWrite+","+Phantom && got != Phantom+","+LostWrite {
		t.Fatalf("got %v", anomalies)
	}
}

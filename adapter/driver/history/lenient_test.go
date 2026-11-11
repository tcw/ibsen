package history

import (
	"strings"
	"testing"

	"github.com/tcw/ibsen/core/domain"
)

// Lenient mode: an acknowledged write is owed only once a flush of the same log covers it.

type instance struct{ name string }

var (
	before = &instance{"before the power cut"}
	after  = &instance{"after it"}
)

func on(log *instance, op Op) Op {
	op.log = log
	return op
}

func flushOf(log *instance, invoke, complete int64, err error) Op {
	return Op{Kind: Flush, Process: 9, Invoke: invoke, Complete: complete, Err: err, log: log}
}

func checkLenient(ops []Op, log map[domain.TopicName][]Entry) []Anomaly {
	return CheckWith(ops, log, Options{Lenient: true})
}

func TestCheckLenient_acceptsWhatLenientModePromises(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []Op
		log  map[domain.TopicName][]Entry
	}{
		{"an unflushed write lost",
			[]Op{on(before, write(1, 1, 2, nil, "a"))},
			logOf()},
		{"an unflushed write kept",
			[]Op{on(before, write(1, 1, 2, nil, "a"))},
			logOf("a")},
		{"an unflushed write kept in part",
			[]Op{on(before, write(1, 1, 2, nil, "a", "b"))},
			logOf("a")},
		{"flushed writes kept, the tail after the flush lost",
			[]Op{on(before, write(1, 1, 2, nil, "a")), flushOf(before, 3, 4, nil),
				on(before, write(1, 5, 6, nil, "b")), on(before, write(1, 7, 8, nil, "c"))},
			logOf("a")},
		{"a failed flush covers nothing",
			[]Op{on(before, write(1, 1, 2, nil, "a")), flushOf(before, 3, 4, errIndeterminate)},
			logOf()},
		{"a flush of another log covers nothing",
			[]Op{on(before, write(1, 1, 2, nil, "a")), flushOf(after, 3, 4, nil),
				on(after, write(1, 5, 6, nil, "b")), flushOf(after, 7, 8, nil)},
			logOf("b")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if anomalies := checkLenient(tc.ops, tc.log); len(anomalies) != 0 {
				t.Fatalf("a valid lenient history was rejected: %v", anomalies)
			}
		})
	}
}

func TestCheckLenient_findsWhatLenientModeStillForbids(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		ops  []Op
		log  map[domain.TopicName][]Entry
	}{
		{"a flushed write lost", LostWrite,
			[]Op{on(before, write(1, 1, 2, nil, "a")), flushOf(before, 3, 4, nil)},
			logOf()},
		{"a reader saw a write the power cut took", ReadNotInLog,
			[]Op{on(before, write(1, 1, 2, nil, "a")), on(before, read(2, 3, 4, 0, "a"))},
			logOf()},
		{"an unflushed write lost while a later one survived", LostThenKept,
			[]Op{on(before, write(1, 1, 2, nil, "a")), on(before, write(1, 3, 4, nil, "b"))},
			logOf("b")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anomalies := checkLenient(tc.ops, tc.log)
			found := false
			for _, anomaly := range anomalies {
				if anomaly.Kind == tc.want {
					found = true
				}
			}
			if !found {
				var got []string
				for _, anomaly := range anomalies {
					got = append(got, anomaly.String())
				}
				t.Fatalf("want %s, got [%s]", tc.want, strings.Join(got, "; "))
			}
		})
	}
}

// The same lost write is a lost write when the checker is not told the mode was lenient.
func TestCheck_strictStillOwesEveryAcknowledgedWrite(t *testing.T) {
	ops := []Op{on(before, write(1, 1, 2, nil, "a"))}
	anomalies := Check(ops, logOf())
	if len(anomalies) == 0 || anomalies[0].Kind != LostWrite {
		t.Fatalf("strict checking accepted a lost acknowledged write: %v", anomalies)
	}
}

package topic

import (
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
)

// DefaultProducerWindowBlocks is how many of a topic's newest blocks a producer is remembered
// across when the caller does not say. A producer that has written nothing for longer is
// forgotten, and its next write is taken as a first one, whatever its number.
const DefaultProducerWindowBlocks = 4

// WriteRequest is one write: the entries, and optionally the producer sending them and its
// number for this write. A zero Producer is a write with no retry protection, exactly as
// before producers existed.
type WriteRequest struct {
	Entries  domain.EntriesPtr
	Producer domain.ProducerID
	Sequence uint64
}

// producerState is what a topic remembers of one producer: the last write it accepted, where
// that write's entries are, and which block they went into, which is how long it is kept.
type producerState struct {
	sequence uint64
	first    domain.Offset
	count    uint32
	block    domain.LogBlock
}

// producerLocked is what the topic remembers of a producer, if it still does. Called with t.mu
// held.
func (t *Topic) producerLocked(id domain.ProducerID) (producerState, bool) {
	state, ok := t.producers[id]
	if !ok || state.block < t.producerWindowStartLocked() {
		return producerState{}, false
	}
	return state, true
}

// producerWindowStartLocked is the first block a producer is remembered from: the oldest of the
// newest ProducerWindowBlocks blocks.
func (t *Topic) producerWindowStartLocked() domain.LogBlock {
	n := len(t.LogBlockList)
	if n == 0 {
		return 0
	}
	start := n - t.ProducerWindowBlocks
	if start < 0 {
		start = 0
	}
	return t.LogBlockList[start]
}

// checkSequenceLocked decides what a producer's write is: a duplicate of the last one, which
// returns where it was stored; the next one, or the first one from a producer this topic does
// not remember, which goes ahead; or anything else, which is refused.
func (t *Topic) checkSequenceLocked(req WriteRequest) (domain.WriteResult, bool, error) {
	if req.Producer.IsZero() {
		return domain.WriteResult{}, false, nil
	}
	state, known := t.producerLocked(req.Producer)
	if !known {
		return domain.WriteResult{}, false, nil
	}
	switch req.Sequence {
	case state.sequence:
		if req.Entries != nil && len(*req.Entries) != int(state.count) {
			// the same number on a different write is a client bug, and answering it as a
			// duplicate would tell the client entries were stored that never were
			return domain.WriteResult{}, false, domain.NewSequenceReusedError(req.Producer, req.Sequence, state.count, len(*req.Entries))
		}
		return domain.WriteResult{First: state.first, Count: state.count, Duplicate: true}, true, nil
	case state.sequence + 1:
		return domain.WriteResult{}, false, nil
	default:
		return domain.WriteResult{}, false, domain.NewSequenceError(req.Producer, req.Sequence, state.sequence)
	}
}

// recordProducerLocked remembers a producer's accepted write.
func (t *Topic) recordProducerLocked(req WriteRequest, first domain.Offset, count int, block domain.LogBlock) {
	if req.Producer.IsZero() {
		return
	}
	if t.producers == nil {
		t.producers = make(map[domain.ProducerID]producerState)
	}
	t.producers[req.Producer] = producerState{sequence: req.Sequence, first: first, count: uint32(count), block: block}
}

// forgetOldProducersLocked drops producers whose last write is older than the window, so the
// table holds what a rebuild after a restart would, and no more.
func (t *Topic) forgetOldProducersLocked() {
	start := t.producerWindowStartLocked()
	for id, state := range t.producers {
		if state.block < start {
			delete(t.producers, id)
		}
	}
}

// rebuildProducersLocked fills the table from the frame headers of the newest blocks, which is
// what makes a retry after a restart recognised. It reads headers only. Called once the head
// block has been recovered, so a write cut short is already gone.
func (t *Topic) rebuildProducersLocked() error {
	t.producers = make(map[domain.ProducerID]producerState)
	start := len(t.LogBlockList) - t.ProducerWindowBlocks
	if start < 0 {
		start = 0
	}
	for _, block := range t.LogBlockList[start:] {
		writes, err := logfmt.ScanProducers(t.Store, t.logRef(block))
		if err != nil {
			return err
		}
		for _, write := range writes {
			t.producers[write.Producer] = producerState{sequence: write.Sequence, first: write.First,
				count: write.Entries, block: block}
		}
	}
	return nil
}

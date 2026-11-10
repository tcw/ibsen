// Package driver holds the driving (inbound) ports: what the core offers to whatever drives
// it. A gRPC server, a CLI or an embedded program calling the log as a library all speak
// these, and the core does not know which of them it is talking to.
package driver

import (
	"sync"

	"github.com/tcw/ibsen/core/domain"
)

// ReadParams is one read: where to start, how much to batch, where the entries go and how
// the caller takes them back.
type ReadParams struct {
	TopicName domain.TopicName
	LogChan   chan *[]domain.LogEntry
	Wg        *sync.WaitGroup
	From      domain.Offset
	BatchSize uint32
	// Cancel stops the read when closed; nil never cancels.
	Cancel <-chan struct{}
	// FromEarliest starts at the earliest offset still retained when From is older than it.
	// Without it such a read fails with domain.ErrOffsetTooOld, which names that offset.
	FromEarliest bool
}

// WriteParams is one write: the topic, the entries, and optionally the producer sending them
// and its number for this write. A write with no producer behaves as writes always have.
type WriteParams struct {
	Topic   domain.TopicName
	Entries domain.EntriesPtr
	// Producer and Sequence make a retry safe. A producer numbers its writes to a topic 1, 2,
	// 3; sent again with the number last accepted, a write is reported as already written,
	// at the offsets it was stored at, and nothing is appended. A number that is neither that
	// nor the next is refused with domain.ErrSequenceGap or domain.ErrStaleSequence. The first
	// write from a producer the topic does not remember may carry any number.
	Producer domain.ProducerID
	Sequence uint64
}

// LogManager is the driving port: the whole of what a log server offers. Write returns where
// the entries went.
type LogManager interface {
	List() []domain.TopicName
	Write(params WriteParams) (domain.WriteResult, error)
	Read(params ReadParams) error
}

// Flusher is the optional capability to make everything written so far durable, and so
// readable, now. It sits beside LogManager rather than in it, the way driven.Syncable sits
// beside BlockStore: the three calls are the whole of what a log offers its clients, and when
// to flush is a decision for whoever runs the log, not for every client of it. In lenient
// flush mode it is the only thing that flushes; in strict mode it waits for flushes already
// under way.
type Flusher interface {
	Flush() error
}

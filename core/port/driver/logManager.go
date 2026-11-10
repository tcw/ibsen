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

// LogManager is the driving port: the whole of what a log server offers.
type LogManager interface {
	List() []domain.TopicName
	Write(topic domain.TopicName, entries domain.EntriesPtr) error
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

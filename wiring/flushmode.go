package wiring

import (
	"errors"
	"time"

	"github.com/tcw/ibsen/core/topic"
	"github.com/tcw/ibsen/errore"
)

// The flush modes --flushMode names.
const (
	// FlushLenient acknowledges a write once it is in the page cache and fsyncs every flush
	// interval: fast, and a power cut can take back what was acknowledged since the last tick.
	FlushLenient = "lenient"
	// FlushStrict acknowledges a write once the fsync covering it has returned.
	FlushStrict = "strict"
)

// DefaultFlushMode is what a deployment that says nothing gets.
const DefaultFlushMode = FlushLenient

// DefaultFlushInterval is the tick in lenient mode when nothing else is given.
const DefaultFlushInterval = time.Second

// FlushModes is every name --flushMode accepts.
var FlushModes = []string{FlushLenient, FlushStrict}

// ErrUnknownFlushMode is returned for a --flushMode name that is neither. It is refused rather
// than replaced, since a guess at what an acknowledgement means is the wrong kind of guess.
var ErrUnknownFlushMode = errors.New("unknown flush mode")

// resolveFlushMode turns a name and an interval into what the core is given. An empty name is
// DefaultFlushMode. In lenient mode the interval is the tick, and zero means
// DefaultFlushInterval: a programmatic caller that set nothing gets a working log. The CLI
// refuses an explicit zero before it gets here, since there it is a mistake.
func resolveFlushMode(name string, interval time.Duration) (topic.FlushMode, time.Duration, error) {
	if interval < 0 {
		return 0, 0, errore.NewF("the flush interval cannot be negative, got %s", interval)
	}
	switch name {
	case "", FlushLenient:
		if interval == 0 {
			interval = DefaultFlushInterval
		}
		return topic.FlushLenient, interval, nil
	case FlushStrict:
		return topic.FlushStrict, interval, nil
	default:
		return 0, 0, errore.WrapWithContextF(ErrUnknownFlushMode, "%q is not one of %v", name, FlushModes)
	}
}

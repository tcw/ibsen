package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ProducerID names one writer for safe retries: a client picks one, numbers its writes to a
// topic 1, 2, 3, and a write sent again with the number already accepted is recognised rather
// than stored twice. The zero ID is no producer, which is every write that does not ask for
// this.
type ProducerID [16]byte

// IsZero reports whether the ID is no producer.
func (p ProducerID) IsZero() bool {
	return p == ProducerID{}
}

// String is the ID as a UUID is written.
func (p ProducerID) String() string {
	h := hex.EncodeToString(p[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// ParseProducerID reads an ID written as a UUID, with or without its hyphens. An empty string
// is no producer.
func ParseProducerID(text string) (ProducerID, error) {
	var id ProducerID
	compact := strings.ReplaceAll(strings.TrimSpace(text), "-", "")
	if compact == "" {
		return id, nil
	}
	if len(compact) != 32 {
		return id, fmt.Errorf("%w: %q is not 16 bytes written as hex or a UUID", ErrInvalidProducerID, text)
	}
	if _, err := hex.Decode(id[:], []byte(compact)); err != nil {
		return id, fmt.Errorf("%w: %q is not 16 bytes written as hex or a UUID", ErrInvalidProducerID, text)
	}
	return id, nil
}

// ProducerIDFromBytes reads an ID from its 16 bytes; no bytes is no producer.
func ProducerIDFromBytes(b []byte) (ProducerID, error) {
	var id ProducerID
	if len(b) == 0 {
		return id, nil
	}
	if len(b) != len(id) {
		return id, fmt.Errorf("%w: %d bytes, want 16", ErrInvalidProducerID, len(b))
	}
	copy(id[:], b)
	return id, nil
}

var (
	// ErrInvalidProducerID is a producer ID that is not 16 bytes.
	ErrInvalidProducerID = errors.New("invalid producer id")

	// ErrSequenceGap is a write numbered more than one past the last one this topic accepted
	// from its producer: a write in between was never sent, or was lost on the client's side.
	ErrSequenceGap = errors.New("sequence number skips past the next one expected")

	// ErrStaleSequence is a write numbered below the last one this topic accepted from its
	// producer: a retry of something older than the latest write, which a client that waits
	// for each write never sends.
	ErrStaleSequence = errors.New("sequence number is older than the last one accepted")
)

// ErrSequenceReused is a write numbered like the last one accepted from its producer, but
// holding a different number of entries: the number was reused for a different write.
var ErrSequenceReused = errors.New("sequence number reused for a different write")

// NewSequenceReusedError says a producer sent its last number again with a different write.
func NewSequenceReusedError(producer ProducerID, sequence uint64, stored uint32, sent int) error {
	return fmt.Errorf("%w: producer %s sequence %d holds %d entries, and the retry %d",
		ErrSequenceReused, producer, sequence, stored, sent)
}

// SequenceError says which number a producer sent and which the topic expected.
type SequenceError struct {
	Producer ProducerID
	Sent     uint64
	Last     uint64
	stale    bool
}

func (e *SequenceError) Error() string {
	kind := "skips ahead of"
	if e.stale {
		kind = "is older than"
	}
	return fmt.Sprintf("producer %s sent sequence %d, which %s %d, the last one accepted; the next is %d",
		e.Producer, e.Sent, kind, e.Last, e.Last+1)
}

func (e *SequenceError) Is(target error) bool {
	if e.stale {
		return target == ErrStaleSequence
	}
	return target == ErrSequenceGap
}

// NewSequenceError says a producer's number was neither the last one nor the next.
func NewSequenceError(producer ProducerID, sent, last uint64) error {
	return &SequenceError{Producer: producer, Sent: sent, Last: last, stale: sent < last}
}

// WriteResult is where a write's entries are: the first offset and how many. Duplicate says
// the write was a retry of one already stored, which is where these offsets point, and that
// nothing was appended this time.
type WriteResult struct {
	First     Offset
	Count     uint32
	Duplicate bool
}

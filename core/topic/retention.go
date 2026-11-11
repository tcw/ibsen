package topic

import (
	"errors"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

// RetentionPolicy says how much of a topic to keep. A block is deleted when either limit says
// so; a zero limit says nothing. The zero policy keeps everything.
type RetentionPolicy struct {
	// MaxAge deletes a block once its last entry is older than this. A store that cannot tell
	// when a block changed reports zero, and such a block is never deleted for its age.
	MaxAge time.Duration
	// MaxBytes deletes the oldest blocks until the topic's log blocks hold at most this many
	// bytes, or only the head is left.
	MaxBytes int64
}

// Keeps reports whether the policy keeps everything.
func (p RetentionPolicy) Keeps() bool {
	return p.MaxAge <= 0 && p.MaxBytes <= 0
}

// Retained is what one Retain removed.
type Retained struct {
	// Blocks are the log blocks removed, oldest first.
	Blocks []domain.LogBlock
	// Bytes is how many log bytes they held.
	Bytes int64
	// Earliest is the earliest offset still in the topic.
	Earliest domain.Offset
}

// Retain deletes whole log blocks, oldest first, while the policy says to, and never the head.
// It takes now as an argument rather than reading a clock, so the core needs none and a test
// can be any age it likes.
//
// Only a prefix of the log is ever removed, so the offsets left are still contiguous. The
// block lists change first, under the topic's lock, which is the lock indexing and writing take
// as well; then each log block is removed, and then its index. A crash between the two leaves
// an index with no log, which loading ignores and removes. A reader holding a snapshot taken
// before is told ErrOffsetTooOld when it reaches a block that is gone, never handed a partial
// one.
func (t *Topic) Retain(policy RetentionPolicy, now time.Time) (Retained, error) {
	if policy.Keeps() {
		return Retained{}, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.LogBlockList) < 2 {
		return Retained{Earliest: t.earliestLocked()}, nil
	}
	blocks, err := t.Store.List(t.topic(), driven.Log)
	if err != nil {
		return Retained{}, errore.Wrap(err)
	}
	head, _ := t.logBlockHead()
	var total int64
	for _, block := range blocks {
		total += block.Size
	}
	var removed Retained
	for _, block := range blocks {
		if domain.LogBlock(block.Block) >= head {
			break
		}
		old := policy.MaxAge > 0 && !block.Modified.IsZero() && now.Sub(block.Modified) > policy.MaxAge
		large := policy.MaxBytes > 0 && total > policy.MaxBytes
		if !old && !large {
			break
		}
		total -= block.Size
		removed.Blocks = append(removed.Blocks, domain.LogBlock(block.Block))
		removed.Bytes += block.Size
	}
	if len(removed.Blocks) == 0 {
		return Retained{Earliest: t.earliestLocked()}, nil
	}

	last := removed.Blocks[len(removed.Blocks)-1]
	t.LogBlockList = dropLogBlocksThrough(t.LogBlockList, last)
	t.IndexBlockList = dropIndexBlocksThrough(t.IndexBlockList, domain.IndexBlock(last))
	removed.Earliest = t.earliestLocked()

	for _, block := range removed.Blocks {
		if err := t.Store.Remove(t.logRef(block)); err != nil && !errors.Is(err, driven.ErrBlockNotFound) {
			return removed, errore.WrapWithContextF(err, "unable to remove log block %d", block)
		}
		if err := t.Store.Remove(t.indexRef(domain.IndexBlock(block))); err != nil && !errors.Is(err, driven.ErrBlockNotFound) {
			return removed, errore.WrapWithContextF(err, "unable to remove index block %d", block)
		}
	}
	// a dictionary goes only once no block left names it
	if err := t.removeUnusedDictionariesLocked(); err != nil {
		return removed, errore.WrapWithContextF(err, "unable to remove unused dictionaries")
	}
	t.Log.Log(driven.LevelInfo, "retention removed log blocks",
		driven.Str("topic", t.TopicName), driven.Int("blocks", len(removed.Blocks)),
		driven.Int64("bytes", removed.Bytes), driven.Uint64("earliestOffset", uint64(removed.Earliest)))
	return removed, nil
}

// Earliest is the earliest offset the topic still holds.
func (t *Topic) Earliest() domain.Offset {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.earliestLocked()
}

func (t *Topic) earliestLocked() domain.Offset {
	if len(t.LogBlockList) == 0 {
		return 0
	}
	// a block is named by its first offset
	return domain.Offset(t.LogBlockList[0])
}

func dropLogBlocksThrough(list []domain.LogBlock, last domain.LogBlock) []domain.LogBlock {
	kept := list[:0:0]
	for _, block := range list {
		if block > last {
			kept = append(kept, block)
		}
	}
	return kept
}

func dropIndexBlocksThrough(list []domain.IndexBlock, last domain.IndexBlock) []domain.IndexBlock {
	kept := list[:0:0]
	for _, block := range list {
		if block > last {
			kept = append(kept, block)
		}
	}
	return kept
}

// earliestInStore is the first offset the store still holds for the topic, for a reader whose
// snapshot is older than a retention run.
func (t *Topic) earliestInStore() domain.Offset {
	blocks, err := t.Store.List(t.topic(), driven.Log)
	if err != nil || len(blocks) == 0 {
		return 0
	}
	return domain.Offset(blocks[0].Block)
}

// orphanIndexes are index blocks older than the first log block: what a crash between
// removing a log block and its index leaves. They index nothing.
func orphanIndexes(logBlocks, indexBlocks []driven.Block) (kept, orphans []driven.Block) {
	if len(logBlocks) == 0 {
		return indexBlocks, nil
	}
	first := logBlocks[0].Block
	for _, block := range indexBlocks {
		if block.Block < first {
			orphans = append(orphans, block)
		} else {
			kept = append(kept, block)
		}
	}
	return kept, orphans
}

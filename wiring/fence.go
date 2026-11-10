package wiring

import (
	"errors"
	"fmt"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

// ErrNotHolding is what a fenced store returns for a write once the single-writer lock can no
// longer be proven held: another instance may be about to take over, and the log must not be
// written by two.
var ErrNotHolding = errors.New("the single-writer lock is not known to be held; refusing to write")

// holder is a lock that can say whether it is still held, with a margin before the moment
// another instance could take over: the Kubernetes lease. A lock without it is not fenced.
type holder interface {
	Held() bool
}

// fence wraps store so every change to it is refused once held says the lock is gone. It is
// the composition root's, not the core's: the ports carry no token, and a lease that can tell
// whether it is held is enough to stop writing before another writer starts.
//
// It checks before each change and again after an append, since a lease can be lost while the
// bytes go down. An append that landed after the lock was lost is rolled back; one that could
// not be is reported wrapped in driven.ErrDirtyBlock, so the core recovers the block before it
// would ever append to it again.
//
// A decorator must forward Syncable, or the core concludes there is nothing to flush and the
// durability guarantee quietly stops holding (ARCHITECTURE.md invariant 8). fence returns a
// type that is Syncable exactly when store is.
func fence(store driven.BlockStore, held func() bool) driven.BlockStore {
	fenced := &fencedStore{BlockStore: store, held: held}
	if syncable, ok := store.(driven.Syncable); ok {
		return &fencedSyncableStore{fencedStore: fenced, syncable: syncable}
	}
	return fenced
}

type fencedStore struct {
	driven.BlockStore
	held func() bool
}

type fencedSyncableStore struct {
	*fencedStore
	syncable driven.Syncable
}

// Sync is forwarded, and not fenced: it writes nothing new, and refusing it would leave
// acknowledged-to-be bytes unsynced for no gain.
func (f *fencedSyncableStore) Sync(ref driven.BlockRef) error {
	return f.syncable.Sync(ref)
}

func (f *fencedStore) refuse(what string, ref fmt.Stringer) error {
	if f.held() {
		return nil
	}
	return fmt.Errorf("%w: %s %s", ErrNotHolding, what, ref)
}

func (f *fencedStore) CreateTopic(topic domain.TopicName) (bool, error) {
	if !f.held() {
		return false, fmt.Errorf("%w: create topic %s", ErrNotHolding, topic)
	}
	return f.BlockStore.CreateTopic(topic)
}

func (f *fencedStore) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	if err := f.refuse("append to", ref); err != nil {
		return driven.Block{}, err
	}
	block, err := f.BlockStore.Append(ref, data)
	if err != nil {
		return block, err
	}
	if f.held() {
		return block, nil
	}
	// lost while the bytes went down: take them back, so the append is all or nothing
	refused := fmt.Errorf("%w: lost while appending to %s", ErrNotHolding, ref)
	if truncErr := f.BlockStore.Truncate(ref, block.Size-int64(len(data))); truncErr != nil {
		return driven.Block{}, driven.DirtyBlock(errors.Join(refused, truncErr))
	}
	return driven.Block{}, refused
}

func (f *fencedStore) Truncate(ref driven.BlockRef, size int64) error {
	if err := f.refuse("truncate", ref); err != nil {
		return err
	}
	return f.BlockStore.Truncate(ref, size)
}

func (f *fencedStore) Remove(ref driven.BlockRef) error {
	if err := f.refuse("remove", ref); err != nil {
		return err
	}
	return f.BlockStore.Remove(ref)
}

// StrayFiles is forwarded when the store has it, so the manager still warns about them.
func (f *fencedStore) StrayFiles(topic domain.TopicName) ([]string, error) {
	if lister, ok := f.BlockStore.(interface {
		StrayFiles(domain.TopicName) ([]string, error)
	}); ok {
		return lister.StrayFiles(topic)
	}
	return nil, nil
}

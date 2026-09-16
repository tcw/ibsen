// Package memstore is a BlockStore that keeps every block in memory. It exists to prove the
// port's shape: it reaches nothing outside the standard library, has no notion of a file,
// and implements no optional capability, so a core that works against it works against a
// backend that can offer nothing but the four block verbs.
package memstore

import (
	"bytes"
	"io"
	"sort"
	"sync"

	"github.com/tcw/ibsen/access/common"
)

// Store keeps blocks as byte slices under a topic.
type Store struct {
	mu     sync.RWMutex
	topics map[common.TopicName]map[blockKey][]byte
}

var _ common.BlockStore = &Store{}

// blockKey is a block within a topic.
type blockKey struct {
	kind  common.BlockKind
	block uint64
}

func New() *Store {
	return &Store{topics: make(map[common.TopicName]map[blockKey][]byte)}
}

func (s *Store) Topics() ([]common.TopicName, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var topics []common.TopicName
	for topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i] < topics[j] })
	return topics, nil
}

func (s *Store) CreateTopic(topic common.TopicName) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.topics[topic]; exists {
		return false, nil
	}
	s.topics[topic] = make(map[blockKey][]byte)
	return true, nil
}

func (s *Store) List(topic common.TopicName, kind common.BlockKind) ([]common.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var blocks []common.Block
	for key, data := range s.topics[topic] {
		if key.kind != kind {
			continue
		}
		blocks = append(blocks, common.Block{Block: key.block, Size: int64(len(data))})
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Block < blocks[j].Block })
	return blocks, nil
}

func (s *Store) Append(ref common.BlockRef, data []byte) (common.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks, exists := s.topics[ref.Topic]
	if !exists {
		blocks = make(map[blockKey][]byte)
		s.topics[ref.Topic] = blocks
	}
	key := keyOf(ref)
	// appending only ever writes past the end of the block, so a reader opened earlier
	// keeps reading the bytes it was given
	blocks[key] = append(blocks[key], data...)
	return common.Block{Block: ref.Block, Size: int64(len(blocks[key]))}, nil
}

func (s *Store) Open(ref common.BlockRef, byteOffset int64) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, exists := s.topics[ref.Topic][keyOf(ref)]
	if !exists {
		return nil, common.ErrBlockNotFound
	}
	if byteOffset < 0 {
		return nil, common.ErrInvalidSize
	}
	if byteOffset > int64(len(data)) {
		byteOffset = int64(len(data))
	}
	return reader{bytes.NewReader(data[byteOffset:])}, nil
}

func (s *Store) Truncate(ref common.BlockRef, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, exists := s.topics[ref.Topic][keyOf(ref)]
	if !exists {
		return common.ErrBlockNotFound
	}
	if size < 0 || size > int64(len(data)) {
		return common.ErrInvalidSize
	}
	// a copy, so later appends cannot overwrite bytes a reader already holds
	s.topics[ref.Topic][keyOf(ref)] = append([]byte(nil), data[:size]...)
	return nil
}

func (s *Store) Remove(ref common.BlockRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks, exists := s.topics[ref.Topic]
	if !exists {
		return common.ErrBlockNotFound
	}
	if _, exists = blocks[keyOf(ref)]; !exists {
		return common.ErrBlockNotFound
	}
	delete(blocks, keyOf(ref))
	return nil
}

func keyOf(ref common.BlockRef) blockKey {
	return blockKey{kind: ref.Kind, block: ref.Block}
}

// reader adds the Close a BlockStore reader owes its caller.
type reader struct {
	*bytes.Reader
}

func (r reader) Close() error {
	return nil
}

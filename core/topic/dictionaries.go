package topic

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"sync"
	"sync/atomic"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

// A topic trains a compression dictionary of its own once it has seen enough entries, when its
// codec can use one. The core samples entries and keeps what comes back; the training itself
// is the codec adapter's, behind driven.DictionaryCodec, so the core stays pure and a build
// without a dictionary-capable codec carries none of this beyond the bookkeeping.
const (
	// DefaultDictTrainBytes is how many plain entry bytes a topic writes before it first
	// trains a dictionary.
	DefaultDictTrainBytes = 4 << 20
	// dictSamples is how many entries the reservoir keeps to train on.
	dictSamples = 1000
	// dictMaxSampleEntry leaves out entries too large to be what a dictionary helps with.
	dictMaxSampleEntry = 16 << 10
	// dictMaxSize is the largest dictionary trained. Measured, smaller is better here: on
	// ~130-byte JSON events a 4 KiB dictionary compressed one-entry frames to 0.80 of their
	// size and ten-entry frames to 0.26, against 0.78 and 0.28 at 64 KiB, and encoded a small
	// frame in a tenth of the time, since zstd primes its window with the whole dictionary on
	// every frame.
	dictMaxSize = 4 << 10
	// dictMinGain is how much smaller a dictionary has to make held-out entries than the
	// codec does without one, for the dictionary to be kept.
	dictMinGain = 0.10
	// dictDegrade is how much worse recent frames may compress than the dictionary did when
	// it was trained, before a new one is trained.
	dictDegrade = 1.25
	// dictMaxEvalChunk caps how many held-out entries are compressed together when a
	// dictionary is judged: as many as the topic's frames have held on average, since a
	// dictionary that pays on one-entry frames can cost on hundred-entry ones.
	dictMaxEvalChunk = 1000
	// dictRecentBytes is the most plain bytes of recent frames the degradation check waits
	// for; a topic that trains sooner judges sooner, at a quarter of its training threshold.
	dictRecentBytes = 1 << 20
)

func (s *dictState) recentWindow() int64 {
	return min(int64(dictRecentBytes), max(s.trainBytes/4, 1))
}

// dictionaries finds a topic's dictionaries in its store, verifying each once and keeping it.
// It is shared by a topic and its read snapshots.
type dictionaries struct {
	store driven.BlockStore
	topic domain.TopicName
	mu    sync.Mutex
	known map[uint64]driven.Dictionary
}

var _ driven.Dictionaries = &dictionaries{}

// StoreDictionaries finds the dictionaries of one topic in a store, for a reader outside any
// topic, such as a tool reading a block file.
func StoreDictionaries(store driven.BlockStore, topic domain.TopicName) driven.Dictionaries {
	return newDictionaries(store, topic)
}

func newDictionaries(store driven.BlockStore, topic domain.TopicName) *dictionaries {
	return &dictionaries{store: store, topic: topic, known: map[uint64]driven.Dictionary{}}
}

// Dictionary returns the dictionary with id, reading and verifying it the first time. A
// missing or corrupt one is an error naming it, and changes nothing on disk.
func (d *dictionaries) Dictionary(id uint64) (driven.Dictionary, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if dict, ok := d.known[id]; ok {
		return dict, nil
	}
	reader, err := d.store.Open(driven.DictRef(d.topic, id), 0)
	if errors.Is(err, driven.ErrBlockNotFound) {
		return driven.Dictionary{}, fmt.Errorf("%w: dictionary %016x of topic %s", driven.ErrDictionaryNotFound, id, d.topic)
	}
	if err != nil {
		return driven.Dictionary{}, errore.Wrap(err)
	}
	content, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return driven.Dictionary{}, errore.Wrap(err)
	}
	if len(content) < 4 || binary.LittleEndian.Uint32(content) != crc32.Checksum(content[4:], crcTable) {
		return driven.Dictionary{}, fmt.Errorf("%w: dictionary %016x of topic %s does not verify", driven.ErrCorruptDictionary, id, d.topic)
	}
	dict := driven.Dictionary{ID: id, Bytes: content[4:]}
	if dictionaryID(dict.Bytes) != id {
		return driven.Dictionary{}, fmt.Errorf("%w: dictionary %016x of topic %s holds the bytes of another", driven.ErrCorruptDictionary, id, d.topic)
	}
	d.known[id] = dict
	return dict, nil
}

// save writes a dictionary and makes it durable, name and all, before anything may use it.
// The id is the hash of the bytes, so saving one that is already there changes nothing.
func (d *dictionaries) save(dict driven.Dictionary) error {
	if _, err := d.Dictionary(dict.ID); err == nil {
		return nil
	}
	encoded := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(dict.Bytes, crcTable))
	encoded = append(encoded, dict.Bytes...)
	ref := driven.DictRef(d.topic, dict.ID)
	if _, err := d.store.Append(ref, encoded); err != nil {
		return errore.WrapWithContextF(err, "unable to write dictionary %016x", dict.ID)
	}
	if _, err := driven.Sync(d.store, ref); err != nil {
		return errore.WrapWithContextF(err, "unable to make dictionary %016x durable", dict.ID)
	}
	d.mu.Lock()
	d.known[dict.ID] = dict
	d.mu.Unlock()
	return nil
}

// forget drops a removed dictionary from the cache.
func (d *dictionaries) forget(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.known, id)
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// dictionaryID is a dictionary's name: a hash of its bytes, so the name is a promise about
// them, and never zero, which means none. FNV-1a: the names are compared, never attacked, and
// a cryptographic hash would cost an embedded build a fifth of a megabyte for nothing.
func dictionaryID(content []byte) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write(content)
	id := hash.Sum64()
	if id == 0 {
		id = 1
	}
	return id
}

// dictState is a topic's training state, guarded by Topic.mu.
type dictState struct {
	codec      driven.DictionaryCodec
	trainBytes int64
	current    *driven.Dictionary
	// samples is the reservoir of entries to train on; seen is how many were offered
	samples [][]byte
	seen    uint64
	rng     uint64
	// plainSince counts plain bytes written since the last training, or since loading
	plainSince int64
	// trainedRatio is how well the current dictionary compressed held-out entries; recent*
	// are how well it has done since
	trainedRatio float64
	recentPlain  int64
	recentStored int64
	// frames and framedEntries count what the topic's frames have held since the last
	// training, which is the frame size a dictionary is judged at
	frames        int64
	framedEntries int64
	// trainedIn is the head block when training last ran: at most once per block
	trainedIn  domain.LogBlock
	hasTrained bool
	// training is 1 while a run is under way
	training int32
	wg       sync.WaitGroup
}

// sampleLocked offers entries to the reservoir: every entry has the same chance of being in
// it, however many came before, which is reservoir sampling (Algorithm R). An entry is kept
// as a frame holds it — checksum, size and offset around the bytes — since that is what a
// dictionary is applied to; one trained on the bare entries matches frames worse.
func (s *dictState) sampleLocked(entries [][]byte, first domain.Offset) {
	for i, entry := range entries {
		if len(entry) == 0 || len(entry) > dictMaxSampleEntry {
			continue
		}
		s.seen++
		if len(s.samples) < dictSamples {
			s.samples = append(s.samples, domain.AppendEntry(nil, entry, first+domain.Offset(i)))
			continue
		}
		// xorshift: the core takes no randomness from outside, and needs none that is good
		s.rng ^= s.rng << 13
		s.rng ^= s.rng >> 7
		s.rng ^= s.rng << 17
		if slot := s.rng % s.seen; slot < dictSamples {
			s.samples[slot] = domain.AppendEntry(s.samples[slot][:0], entry, first+domain.Offset(i))
		}
	}
}

// dueLocked says whether a training run should start: the first once enough bytes were
// written, and later ones when the current dictionary has stopped earning its keep, at most
// once per block.
func (s *dictState) dueLocked(head domain.LogBlock) bool {
	if atomic.LoadInt32(&s.training) == 1 || len(s.samples) < dictSamples/10 {
		return false
	}
	if s.hasTrained && s.trainedIn == head {
		return false
	}
	if s.current == nil {
		return s.plainSince >= s.trainBytes
	}
	if s.recentPlain < s.recentWindow() || s.trainedRatio == 0 {
		return false
	}
	return float64(s.recentStored)/float64(s.recentPlain) > s.trainedRatio*dictDegrade
}

// noteFrameLocked records how well a frame written with the current dictionary compressed.
// The first stretch after a load, when there is no trained ratio to compare against, becomes
// the baseline.
func (s *dictState) noteFrameLocked(plain, stored int) {
	s.recentPlain += int64(plain)
	s.recentStored += int64(stored)
	if s.trainedRatio == 0 && s.recentPlain >= s.recentWindow() {
		s.trainedRatio = float64(s.recentStored) / float64(s.recentPlain)
		s.recentPlain, s.recentStored = 0, 0
	}
}

// startTrainingLocked starts a training run when one is due. The run is off the write path: the
// write that crossed the threshold starts it and returns, like indexing, and no timer is
// involved. Close waits for it.
func (t *Topic) startTrainingLocked(head domain.LogBlock) {
	s := t.dict
	if s == nil || t.closed || !s.dueLocked(head) {
		return
	}
	if !atomic.CompareAndSwapInt32(&s.training, 0, 1) {
		return
	}
	samples := s.samples
	chunk := 1
	if s.frames > 0 {
		chunk = int(min(max(s.framedEntries/s.frames, 1), dictMaxEvalChunk))
	}
	s.frames, s.framedEntries = 0, 0
	s.samples, s.seen = nil, 0
	s.plainSince, s.recentPlain, s.recentStored = 0, 0, 0
	s.trainedIn, s.hasTrained = head, true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer atomic.StoreInt32(&s.training, 0)
		if err := t.train(samples, chunk); err != nil {
			t.Log.Log(driven.LevelWarn, "training a compression dictionary failed; frames are written without one",
				driven.Str("topic", t.TopicName), driven.Err(err))
		}
	}()
}

// train builds a dictionary from samples, keeps it only if it pays on entries held out of the
// training, and makes it the one new frames are written with once it is durable.
func (t *Topic) train(samples [][]byte, chunk int) error {
	codec := t.dict.codec
	held := len(samples) / 10
	heldOut, training := samples[:held], samples[held:]
	content, err := codec.Train(training, dictMaxSize)
	if err != nil {
		return err
	}
	dict := driven.Dictionary{ID: dictionaryID(content), Bytes: content}
	plain, without, with, err := judgeDictionary(codec, dict, heldOut, chunk)
	if err != nil {
		return err
	}
	if float64(with) > float64(without)*(1-dictMinGain) {
		t.Log.Log(driven.LevelDebug, "a trained dictionary did not pay, writing without one",
			driven.Str("topic", t.TopicName), driven.Int64("withDictionary", with), driven.Int64("without", without))
		return nil
	}
	if err := t.dicts.save(dict); err != nil {
		return err
	}
	t.mu.Lock()
	t.dict.current = &dict
	t.dict.trainedRatio = float64(with) / float64(plain)
	t.mu.Unlock()
	t.Log.Log(driven.LevelInfo, "trained a compression dictionary",
		driven.Str("topic", t.TopicName), driven.Uint64("dictionary", dict.ID), driven.Int("bytes", len(content)),
		driven.Int64("heldOutPlain", plain), driven.Int64("withDictionary", with), driven.Int64("without", without))
	return nil
}

// judgeDictionary compresses held-out entries in groups of size, the topic's frame size, with
// and without dict.
func judgeDictionary(codec driven.DictionaryCodec, dict driven.Dictionary, heldOut [][]byte, size int) (plain, without, with int64, err error) {
	for start := 0; start < len(heldOut); start += size {
		var chunk []byte
		for _, entry := range heldOut[start:min(start+size, len(heldOut))] {
			chunk = append(chunk, entry...)
		}
		plain += int64(len(chunk))
		encoded, err := codec.Encode(nil, chunk)
		if err != nil {
			return 0, 0, 0, err
		}
		without += int64(min(len(encoded), len(chunk)))
		encoded, err = codec.EncodeDict(nil, chunk, dict)
		if err != nil {
			return 0, 0, 0, err
		}
		with += int64(min(len(encoded), len(chunk)))
	}
	if plain == 0 {
		return 0, 0, 0, errors.New("nothing held out to judge the dictionary on")
	}
	return plain, without, with, nil
}

// loadCurrentDictionaryLocked finds the dictionary the newest frames were written with, which
// is the one to carry on with. A dictionary no frame names — one written just before a crash —
// is not taken up; the next training finds or makes one again.
func (t *Topic) loadCurrentDictionaryLocked() error {
	if t.dict == nil {
		return nil
	}
	start := max(0, len(t.LogBlockList)-t.ProducerWindowBlocks)
	for i := len(t.LogBlockList) - 1; i >= start; i-- {
		ids, err := logfmt.ScanDictionaries(t.Store, t.logRef(t.LogBlockList[i]))
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			continue
		}
		dict, err := t.dicts.Dictionary(ids[len(ids)-1])
		if err != nil {
			// the frames that name it say what is wrong when they are read; writing goes on
			// without it rather than refusing to load
			t.Log.Log(driven.LevelWarn, "the newest frames name a dictionary that cannot be read",
				driven.Str("topic", t.TopicName), driven.Err(err))
			return nil
		}
		t.dict.current = &dict
		return nil
	}
	return nil
}

// removeUnusedDictionariesLocked removes dictionaries no remaining log block names and that
// are not the one being written with. Called by retention, once blocks are gone.
func (t *Topic) removeUnusedDictionariesLocked() error {
	stored, err := t.Store.List(t.topic(), driven.Dict)
	if err != nil || len(stored) == 0 {
		return err
	}
	used := map[uint64]bool{}
	if t.dict != nil && t.dict.current != nil {
		used[t.dict.current.ID] = true
	}
	for _, block := range t.LogBlockList {
		ids, err := logfmt.ScanDictionaries(t.Store, t.logRef(block))
		if errors.Is(err, driven.ErrBlockNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		for _, id := range ids {
			used[id] = true
		}
	}
	for _, block := range stored {
		if used[block.Block] {
			continue
		}
		if err := t.Store.Remove(driven.DictRef(t.topic(), block.Block)); err != nil && !errors.Is(err, driven.ErrBlockNotFound) {
			return err
		}
		t.dicts.forget(block.Block)
	}
	return nil
}

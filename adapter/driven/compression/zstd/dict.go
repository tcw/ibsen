package zstd

import (
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/tcw/ibsen/core/port/driven"
)

var _ driven.DictionaryCodec = &Codec{}

// dictCoders are the encoder and decoder built for one dictionary. Building them is what
// costs, and a dictionary id never changes its bytes, so they are built once per id and kept.
type dictCoders struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

type dictCache struct {
	mu     sync.Mutex
	coders map[uint64]*dictCoders
}

// DictCodecID is the byte a frame encoded with a dictionary carries.
func (c *Codec) DictCodecID() driven.CodecID { return driven.CodecZstdDict }

// minSamples is the fewest samples worth training on; fewer says nothing about a topic.
const minSamples = 8

// Train builds a zstd dictionary from samples: their bytes, newest last, as the history every
// frame can refer back to, with entropy tables tuned on the samples themselves. It is built
// for this codec's level, which is a matter of speed only: the dictionary reads back at any.
func (c *Codec) Train(samples [][]byte, maxSize int) ([]byte, error) {
	if len(samples) < minSamples {
		return nil, fmt.Errorf("%d samples are too few to train a dictionary on", len(samples))
	}
	// zstd looks back from the end of the history, so the samples go in oldest first and the
	// history is cut from the front when it is too long
	var history []byte
	for _, sample := range samples {
		history = append(history, sample...)
	}
	if len(history) > maxSize {
		history = history[len(history)-maxSize:]
	}
	if len(history) < 8 {
		return nil, errors.New("the samples hold too few bytes to train a dictionary on")
	}
	contents := make([][]byte, len(samples))
	copy(contents, samples)
	return zstd.BuildDict(zstd.BuildDictOptions{
		// zstd's own id for the dictionary; the frame names it by Ibsen's, and a decoder is
		// built per dictionary, so this one only has to be non-zero
		ID:       1,
		Contents: contents,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    c.level,
	})
}

func (c *Codec) codersFor(dict driven.Dictionary) (*dictCoders, error) {
	c.dicts.mu.Lock()
	defer c.dicts.mu.Unlock()
	if coders, ok := c.dicts.coders[dict.ID]; ok {
		return coders, nil
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(c.level), zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderDict(dict.Bytes))
	if err != nil {
		return nil, fmt.Errorf("dictionary %016x: %w", dict.ID, err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(maxDecoderMemory), zstd.WithDecoderDicts(dict.Bytes))
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("dictionary %016x: %w", dict.ID, err)
	}
	if c.dicts.coders == nil {
		c.dicts.coders = map[uint64]*dictCoders{}
	}
	coders := &dictCoders{encoder: encoder, decoder: decoder}
	c.dicts.coders[dict.ID] = coders
	return coders, nil
}

// EncodeDict compresses src onto dst with the dictionary.
func (c *Codec) EncodeDict(dst, src []byte, dict driven.Dictionary) ([]byte, error) {
	coders, err := c.codersFor(dict)
	if err != nil {
		return nil, err
	}
	return coders.encoder.EncodeAll(src, dst), nil
}

// DecodeDict decompresses src onto dst with the dictionary it was encoded with.
func (c *Codec) DecodeDict(dst, src []byte, plainSize int, dict driven.Dictionary) ([]byte, error) {
	if plainSize > maxDecoderMemory {
		return nil, fmt.Errorf("frame decodes to %d bytes, more than the %d this decoder will hold",
			plainSize, maxDecoderMemory)
	}
	coders, err := c.codersFor(dict)
	if err != nil {
		return nil, err
	}
	return coders.decoder.DecodeAll(src, dst)
}

func (d *dictCache) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, coders := range d.coders {
		coders.encoder.Close()
		coders.decoder.Close()
	}
	d.coders = nil
}

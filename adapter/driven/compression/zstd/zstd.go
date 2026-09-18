// Package zstd is the zstd adapter for the driven.Codec port. It is the only thing in the
// tree that knows what zstd is: the core names codecs by a byte and this package answers to
// one of them, so a build that does not wire it links none of it.
package zstd

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/tcw/ibsen/core/port/driven"
)

var _ driven.Codec = &Codec{}

// Codec compresses frame payloads with zstd. One instance serves every topic: the encoder
// and decoder underneath are safe for concurrent use, and both are built once because
// building them is what costs.
type Codec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// Level is how hard the encoder tries. Faster levels cost less CPU per write and store more
// bytes; slower ones the other way round. A level is not part of the format: a frame says
// only that zstd wrote it, so the level can change between runs, or between frames, and
// everything already written stays readable.
type Level string

const (
	Fastest Level = "fastest"
	Default Level = "default"
	Better  Level = "better"
	Best    Level = "best"
)

// maxDecoderMemory bounds what one decoded frame may cost. The frame header already says how
// many bytes should come out and logfmt rejects a codec that returns a different number, so
// this is the belt to that braces: a corrupt payload never gets to allocate first and be
// rejected afterwards.
const maxDecoderMemory = 64 << 20

// New builds the codec at the given level. An unknown level is an error rather than a quiet
// fallback, since it comes from a flag someone typed.
func New(level Level) (*Codec, error) {
	encoderLevel, err := encoderLevel(level)
	if err != nil {
		return nil, err
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(encoderLevel),
		// frames are bounded by the topic, and each is compressed whole by EncodeAll
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(maxDecoderMemory))
	if err != nil {
		encoder.Close()
		return nil, err
	}
	return &Codec{encoder: encoder, decoder: decoder}, nil
}

func encoderLevel(level Level) (zstd.EncoderLevel, error) {
	switch level {
	case Fastest:
		return zstd.SpeedFastest, nil
	case "", Default:
		return zstd.SpeedDefault, nil
	case Better:
		return zstd.SpeedBetterCompression, nil
	case Best:
		return zstd.SpeedBestCompression, nil
	default:
		return 0, fmt.Errorf("unknown zstd level %q, want one of %s, %s, %s, %s",
			level, Fastest, Default, Better, Best)
	}
}

// ID is the byte a frame carries to say zstd wrote it.
func (c *Codec) ID() driven.CodecID { return driven.CodecZstd }

// Encode compresses src onto dst. EncodeAll appends and does not retain src, which is what
// the port asks for.
func (c *Codec) Encode(dst, src []byte) ([]byte, error) {
	return c.encoder.EncodeAll(src, dst), nil
}

// Decode decompresses src onto dst. plainSize is what the frame header says should come out;
// a payload claiming more than the decoder will spend memory on is refused before it is
// decoded rather than after.
func (c *Codec) Decode(dst, src []byte, plainSize int) ([]byte, error) {
	if plainSize > maxDecoderMemory {
		return nil, fmt.Errorf("frame decodes to %d bytes, more than the %d this decoder will hold",
			plainSize, maxDecoderMemory)
	}
	return c.decoder.DecodeAll(src, dst)
}

// Close releases the encoder and decoder. A codec outlives every topic that uses it, so this
// belongs to whoever built it.
func (c *Codec) Close() {
	c.encoder.Close()
	c.decoder.Close()
}

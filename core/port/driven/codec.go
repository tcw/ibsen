package driven

import (
	"errors"
	"fmt"
)

// Codec is the compression port. The core knows a frame payload is a byte slice that goes
// out one way and has to come back the same; it has no opinion on how, and holds no
// compressor of its own beyond the identity one below. An adapter brings zstd, or whatever a
// deployment can afford to link.
//
// A codec is named by one byte, which every frame carries, so a block records what it was
// written with rather than a deployment having to remember. That byte is also why a codec
// may never change what it produces for an id once frames exist: an id is a promise about
// bytes already on disk.
//
// Implementations must be safe for concurrent use: one codec serves every topic.
type Codec interface {
	// ID is the byte a frame header carries to name this codec.
	ID() CodecID

	// Encode compresses src, appending the result to dst and returning it the way append
	// does. It must not retain or modify src.
	Encode(dst, src []byte) ([]byte, error)

	// Decode decompresses src, which Encode produced, appending plainSize bytes to dst and
	// returning it. plainSize comes from the frame header, and a codec that would produce a
	// different number of bytes must report an error rather than return them.
	Decode(dst, src []byte, plainSize int) ([]byte, error)
}

// CodecID names a codec in one byte. The zero value is CodecNone, so a frame written by a
// build with no compression wired reads back on any build.
type CodecID uint8

const (
	// CodecNone stores the payload as it is.
	CodecNone CodecID = 0
	// CodecZstd is zstd, which an adapter provides; the core never links it.
	CodecZstd CodecID = 1
)

func (c CodecID) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecZstd:
		return "zstd"
	default:
		return fmt.Sprintf("CodecID(%d)", uint8(c))
	}
}

// ErrUnknownCodec is a frame written with a codec this build did not wire. The bytes are
// intact and nothing is truncated: the deployment is missing a codec, not the data.
var ErrUnknownCodec = errors.New("unknown codec")

// Codecs is what a read resolves a frame's codec byte against. It is built at wiring time,
// which is what decides how much compression code a binary carries: an embedded build wires
// none and links none.
type Codecs map[CodecID]Codec

// NewCodecs collects codecs into a registry. NoCodec is always in it, since a frame written
// without compression must be readable by every build.
func NewCodecs(codecs ...Codec) Codecs {
	registry := Codecs{CodecNone: NoCodec{}}
	for _, codec := range codecs {
		registry[codec.ID()] = codec
	}
	return registry
}

// Get returns the codec a frame was written with, or ErrUnknownCodec naming the byte it
// asked for.
func (c Codecs) Get(id CodecID) (Codec, error) {
	if codec, ok := c[id]; ok {
		return codec, nil
	}
	if id == CodecNone {
		// a nil or hand-built registry still reads what needs no codec at all
		return NoCodec{}, nil
	}
	return nil, fmt.Errorf("%w: codec %s", ErrUnknownCodec, id)
}

var _ Codec = NoCodec{}

// NoCodec stores payloads as they are. It is the default, and the only codec in the core:
// it needs nothing outside the language, so a build that wires no compression adapter still
// writes and reads frames.
type NoCodec struct{}

func (NoCodec) ID() CodecID { return CodecNone }

func (NoCodec) Encode(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (NoCodec) Decode(dst, src []byte, plainSize int) ([]byte, error) {
	if len(src) != plainSize {
		return nil, fmt.Errorf("stored payload is %d bytes, header says %d plain", len(src), plainSize)
	}
	return append(dst, src...), nil
}

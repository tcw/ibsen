package wiring

import (
	"github.com/tcw/ibsen/core/port/driven"

	zstdcodec "github.com/tcw/ibsen/adapter/driven/compression/zstd"
	"github.com/tcw/ibsen/errore"
)

// buildCodecs turns a compression name into the adapters behind it. Building them here rather
// than in the CLI is what keeps a driving adapter from reaching a driven one: the CLI passes a
// name, and the composition root decides what that name is made of.
//
// A read registry holds every codec this binary links, whatever is written with, so a block
// stays readable after compression is turned off or changed. The codec written with is always
// in it too, including one a caller injected, which is why both are arguments: an injected
// codec is kept rather than replaced.
//
// The zstd codec is returned so its caller can close its buffers, and is closed here if this
// returns an error, since nobody else then has a handle on it.
func buildCodecs(compression string, level string, codec driven.Codec, codecs driven.Codecs) (driven.Codec, driven.Codecs, *zstdcodec.Codec, error) {
	zstd, err := zstdcodec.New(zstdcodec.Level(level))
	if err != nil {
		return nil, nil, nil, errore.Wrap(err)
	}
	if codec == nil {
		switch compression {
		case "", "none":
			codec = driven.NoCodec{}
		case "zstd":
			codec = zstd
		default:
			zstd.Close()
			return nil, nil, nil, errore.WrapWithContextF(ErrUnknownCompression,
				"compression %q, want one of none, zstd", compression)
		}
	}
	if codecs == nil {
		codecs = driven.NewCodecs(zstd)
	}
	if _, taken := codecs[codec.ID()]; !taken {
		codecs[codec.ID()] = codec
	}
	return codec, codecs, zstd, nil
}

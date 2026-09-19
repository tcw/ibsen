package wiring

import (
	"github.com/tcw/ibsen/core/port/driven"

	zstdcodec "github.com/tcw/ibsen/adapter/driven/compression/zstd"
	"github.com/tcw/ibsen/errore"
)

// DefaultCompression is what a deployment writes with when it names no codec: zstd, at the
// default level. A log is mostly text that repeats — the same field names, the same hosts,
// the same shapes of message, entry after entry — and on 1.1 GB of wiki XML it is the
// difference between 1.41 GiB on disk and 456 MiB. What it costs is CPU, and the bytes are
// never stranded: every codec this binary links stays in the read registry, so a block
// written before or after this default reads the same either way.
//
// "none" is one word away for a deployment whose entries are already compressed, or whose
// readers seek rather than stream. See the README.
const DefaultCompression = "zstd"

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
		if compression == "" {
			compression = DefaultCompression
		}
		switch compression {
		case "none":
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

// ReadCodecs is the registry a read resolves a frame's codec byte against: every codec this
// binary links, whatever it writes with. It is what a tool reading a block file outside any
// store needs — without it a frame written with the default codec reads back as an unknown
// one — and the function returned releases what the codecs hold.
func ReadCodecs() (driven.Codecs, func(), error) {
	_, codecs, zstd, err := buildCodecs("none", string(zstdcodec.Default), nil, nil)
	if err != nil {
		return nil, nil, err
	}
	return codecs, zstd.Close, nil
}

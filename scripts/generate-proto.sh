#!/usr/bin/env bash
# Regenerates the Go code for api/grpcApi/ibsen.proto: the server's copy in
# adapter/driver/grpcapi and the client's in clients/go/api/grpcApi.
#
# buf compiles the proto itself, so this needs nothing but Go: no protoc to install, and the
# same output on every machine. buf and the plugins are pinned, by this script and by
# api/buf.gen.yaml. CI runs this and fails if it changes anything, so the two copies cannot
# drift from the proto or from each other.
set -euo pipefail

cd "$(dirname "$0")/.."

go run github.com/bufbuild/buf/cmd/buf@v1.75.0 generate --template api/buf.gen.yaml

#!/usr/bin/env bash
# Builds a release: the checks CI runs, then the binary and the image. The gRPC code is
# generated and committed; regenerate it with clients/go/generate-client.sh.
set -euo pipefail

cd "$(dirname "$0")"

unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
	echo "not gofmt'd:" >&2
	echo "$unformatted" >&2
	exit 1
fi
go vet ./...
./scripts/check-architecture.sh
go test -race ./...

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/ibsen .
docker build --build-arg GO_VERSION="$(awk '/^go /{print $2}' go.mod)" -t ibsen .

#!/bin/bash
# Builds a release: the checks CI runs, then the binary and the image.
set -euo pipefail

cd "$(dirname "$0")"

./scripts/generate-proto.sh
if ! git diff --quiet -- adapter/driver/grpcapi clients/go/api/grpcApi; then
	echo "generated gRPC code was stale and has been regenerated; commit it first" >&2
	exit 1
fi

unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
	echo "not gofmt'd:" >&2
	echo "$unformatted" >&2
	exit 1
fi

go vet ./...
./scripts/check-architecture.sh
./scripts/embedded-size.sh
go test -race ./...

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/ibsen .
docker build -t ibsen .

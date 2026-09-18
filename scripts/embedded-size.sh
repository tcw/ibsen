#!/usr/bin/env bash
# Weighs an embedded build against the full server. This is the other half of how the
# embedded claim is verified: check-architecture.sh reads the dependency graph, this reads
# what the linker actually produced. A tag would have to be trusted; neither of these does.
#
# GOOS and GOARCH pass through, so this also answers "how big is it on the target":
#   GOOS=linux GOARCH=arm scripts/embedded-size.sh
set -euo pipefail

cd "$(dirname "$0")/.."

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# the flags §8 calls for: no cgo, no symbol table, no DWARF
build() {
	CGO_ENABLED=0 go build -ldflags="-s -w" -o "$out/$1" "$2"
}

size() {
	# GNU stat, then BSD stat
	stat -c%s "$out/$1" 2>/dev/null || stat -f%z "$out/$1"
}

build server .
build embedded ./wiring/embedded/example

server=$(size server)
embedded=$(size embedded)

target="${GOOS:-$(go env GOOS)}/${GOARCH:-$(go env GOARCH)}"
# awk rather than bc, which is not on every CI image
awk -v target="$target" -v server="$server" -v embedded="$embedded" 'BEGIN {
	printf "target        %s\n", target
	printf "server        %8.2f MB  (gRPC, cobra, OTEL, zerolog, afero, zstd)\n", server / 1048576
	printf "embedded      %8.2f MB  (core + memstore, standard library only)\n", embedded / 1048576
	printf "embedded is   %8.2fx smaller\n", server / embedded
}'

# A sanity check rather than a budget: a hard byte ceiling would drift with every Go release,
# but an embedded build that stopped being smaller than the server would mean the wiring had
# quietly pulled the server in, and that is worth failing over.
if [ "$embedded" -ge "$server" ]; then
	echo "EMBEDDED BUILD IS NOT SMALLER THAN THE SERVER: the embedded wiring has pulled in something it should not" >&2
	exit 1
fi

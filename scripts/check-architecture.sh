#!/usr/bin/env bash
# Checks the one rule the tree exists to keep: the core is pure, and dependencies point
# inward. Prints nothing and exits 0 when the architecture holds.
set -euo pipefail

status=0

fail() {
	status=1
	echo "ARCHITECTURE VIOLATION: $1" >&2
	shift
	printf '  %s\n' "$@" >&2
}

# Every import of every package in a tree, as "importer -> imported".
edges() {
	go list -f '{{$p := .ImportPath}}{{range .Imports}}{{$p}} -> {{.}}
{{end}}' "$@"
}

# 1. The core, and the adapters that claim to be stdlib-only, may reach nothing outside the
#    standard library. -deps is transitive, so anything errore or utils reached shows up too.
impure=$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' \
	./core/... \
	./adapter/driven/blockstore/memstore/... \
	./adapter/driven/blockstore/flashstore/... \
	./adapter/driven/blockstore/conformance/... |
	{ grep -v '^github.com/tcw/ibsen' || true; })
if [ -n "$impure" ]; then
	fail "the core reaches outside the standard library" $impure
fi

# 2. The core must not know its adapters or the composition root. A port that names an
#    adapter is a port nothing else can implement.
outward=$(edges ./core/... | { grep -E -- '-> github\.com/tcw/ibsen/(adapter|wiring)' || true; })
if [ -n "$outward" ]; then
	fail "a core package imports an adapter or the composition root" "$outward"
fi

# 3. A driving adapter drives the core through its port; it must not reach a driven adapter
#    directly, which would route around the hexagon.
#
#    Two edges are grandfathered in, each a piece of composition that ended up in the wrong
#    place rather than a port being bypassed. Both should move into wiring/; until they do,
#    they are named here so the rule stays enforced for everything else:
#
#      cli -> locking    the CLI builds the FileLock it injects into wiring.IbsenServer
#      grpcapi -> telemetry  StartGRPC launches the OTEL exporter goroutine
#
#    Test-support packages are exempt: a test is its own composition root and wires the
#    adapters it needs.
crosswise=$(edges ./adapter/driver/... |
	{ grep -E -- '-> github\.com/tcw/ibsen/adapter/driven' || true; } |
	{ grep -v -E '^github\.com/tcw/ibsen/adapter/driver/[^ ]*/test ' || true; } |
	{ grep -v -F 'adapter/driver/cli -> github.com/tcw/ibsen/adapter/driven/locking' || true; } |
	{ grep -v -F 'adapter/driver/grpcapi -> github.com/tcw/ibsen/adapter/driven/telemetry' || true; })
if [ -n "$crosswise" ]; then
	fail "a driving adapter imports a driven adapter directly" "$crosswise"
fi

exit $status

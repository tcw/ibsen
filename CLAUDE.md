# Ibsen

## North star

**Keep the core pure. It imports only the standard library, limited to the subset TinyGo supports (no `os`, `net`, or global logger), and speaks only in domain types. Storage, durability, compression, replication, transport, logging, and telemetry are all adapters behind ports, chosen at wiring time in `cmd/`. That one rule is what lets the same log core run on a microcontroller or in a replicated Kubernetes cluster without changing a line of it.**

Enforced in CI (once `core/` exists), must print nothing:

```sh
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./core/... | grep -v '^github.com/tcw/ibsen/core'
```

Everything below hangs off that rule: the bugs are the core earning trust, the ports are the discipline, compression/dictionaries/fencing/embedded builds are adapters and build-time choices behind it, and the migration is how we get there without breaking what works.

## What it is

A Go append-only log server, Kafka-like: topics you write entries to and read back by offset, over gRPC, with a sparse index and block-based storage on afero.

- Entry wire format (`access/common/fsUtils.go` `CreateByteEntry`): `crc32c(4) | size uint64 LE (8) | entry | offset uint64 LE (8)`; CRC covers size, entry, offset.
- Blocks: `<root>/<topic>/%020d.log` and `.idx`, named by first offset. Index file = pairs of `(offset uint64, byteOffset uint64)`.
- Today nothing is pure: `access/*` and `manager` import `afero` and `zerolog`; `access/locking` imports `uuid`. `errore` and `utils` are already stdlib-only.

## Current baseline (verified 2026-09-15, go1.26.4)

- `go test -race ./...` passes (step zero). Run it before and after every migration step.
- Property tests: `access/topicAccess_property_test.go` (read-from-every-offset across block sizes and reload modes; concurrent write/read/index). Stdlib-only, intended to become the `BlockStore` conformance suite.
- `Topic` state is guarded by `Topic.mu`; `Read` works on a `snapshot()` so slow consumers never block writers.
- `go vet` still flags discarded `context.WithTimeout` cancels in `cmd/` and `api/grpcApi/test/` (not yet fixed).

## 1. Correctness bugs (fix first)

From the design session:
1. **Size field width mismatch.** Written as uint64, read as uint32 in the index builder (`access/index/indexUtils.go:138`). Latent rather than active: it still consumes 8 bytes and LE low bits are correct below 4 GiB, but fix it.
2. **Recovery trusts the last 8 bytes** as the final offset (`access/log/logUtils.go:173` `BlockInfo`). Replace with a CRC-validated forward scan that finds the true end of log and truncates any torn tail.

Found while verifying (not in the original summary):
3. **CRC read from the wrong buffer and never verified.** `logUtils.go:279` decodes `bytes` instead of `checksum`; no read path checks CRCs.
4. **Batch byte cap never triggers.** `logUtils.go:317` is `= +int(size)`, not `+=`.
5. **Nil deref on open failure.** `topicAccess.go:254` calls `file.Close()` when `OpenFileForWrite` returned nil.
6. **Index file handle leak.** `topicAccess.go:408` opens the `.idx` for write and never closes it.
7. ~~**Unsynchronized topic state.**~~ Fixed in step zero (`Topic.mu` + read snapshot).
8. **Index resume drops sample points (perf only).** In-process resume starts at end-of-scan and `isFirst` skips that entry; on restart resume starts at the last indexed entry. The two paths disagree on what `IndexPosition.ByteOffset` means.
9. ~~**Cross-block index lookup.**~~ Fixed in step zero. When the newest log block had no index yet (async indexer behind, or `.idx` missing after restart), reads used the previous block's byte offsets to seek in the new block: wrong data, `unexpected EOF`, or a `makeslice` panic. Covered by the `reload-without-newest-index` property mode.

## 2. Durability

fsync-on-flush policy: flush after N entries or a time interval. Only acknowledge a write and advance the visible offset once its flush completes, so readers never see unflushed data. (Today `NextOffset` advances right after `file.Write`, with no sync.)

## 3. Index

- Binary search over the already-sorted offsets instead of the linear scan (`access/index/index.go:53`).
- Make sparsity configurable (hardcoded `10` at `topicAccess.go:400`).
- Checksum index files.

## 4. Architecture: hexagonal refactor

- Pure core, stdlib only, ports in domain terms.
- Key port: narrow **`BlockStore`**, four verbs: append, read-at, list blocks, remove. Deliberately *not* a filesystem abstraction.
- Optional **`Syncable`** capability, probed by type assertion.
- Adapters: afero (one of several), in-memory (tests), raw flash / mmap (embedded).
- gRPC is a driving adapter; on embedded, skip it and call the log as a library.

## 5. Compression

- Per-block compressed **frames**, self-describing: header carries codec, offsets, two CRCs.
- Index points at frame boundaries; reads decompress one frame and scan within it.
- **Frame boundary = flush boundary = durability boundary.**

## 6. Dictionaries

- Per-topic trained zstd dictionaries, versioned by `dictID` in the frame header; immutable; retained while any frame references them.
- Cold start writes plain frames while reservoir-sampling entries; a background trainer builds the dictionary and flips a current pointer.
- Retention now couples dictionary lifetime to log lifetime.

## 7. Distribution

- Don't build Raft. Run on replicated storage as a Kubernetes StatefulSet.
- Keep single-writer safety with a fencing lease and monotonic token (likely etcd lease + mod-revision); storage rejects stale-token writes.
- Coordination is an optional adapter, no-op locally, so embedded doesn't carry it. (Existing `consensus/` + `access/locking` is the seed.)

## 8. Embedded builds

- Build tags select wiring files, but the real lever is import discipline: no heavy dependency reachable from the embedded wiring file.
- Prefer two additive wiring files over exclusions; default build is the full server.
- `CGO_ENABLED=0`, `-ldflags="-s -w"`, cross-compile via `GOOS`/`GOARCH`.
- Verify by diffing binary size and inspecting the dependency graph, not by trusting the tags.

## 9. Testing (the linchpin)

- Shared conformance property-test suite parameterized over any `BlockStore`, run against every adapter.
- Properties: round-trip; read-from-every-offset.
- Crash and torn-write fault injection at the storage adapter.
- Race-detector concurrency tests.
- Cross-dictionary-version read test.

## 10. Migration order: strangler, never two changes at once

0. Property tests green against today's code (including `-race`).
1. Define the port as a thin afero wrapper.
2. Route the core through the port while afero is still the only backend (the invasive change, against a trusted backend).
3. Add the in-memory adapter to prove the port's shape.
4. Add durability and crash tests inside the FS adapter.
5. Add exotic embedded adapters last, validated by the shared suite.

Every step ships green.

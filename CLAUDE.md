# Ibsen

## North star

**Keep the core pure. It imports only the standard library, limited to the subset TinyGo supports (no `os`, `net`, or global logger), and speaks only in domain types. Storage, durability, compression, replication, transport, logging, and telemetry are all adapters behind ports, chosen at wiring time in `wiring/`. That one rule is what lets the same log core run on a microcontroller or in a replicated Kubernetes cluster without changing a line of it.**

Enforced by `scripts/check-architecture.sh`, which CI runs on every push. Its first rule is
this command; it must print nothing. All of `core/` is pure, so the whole hexagon is covered
by one pattern, with the three stdlib-only adapters named beside it:

```sh
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' \
  ./core/... \
  ./adapter/driven/blockstore/memstore/... ./adapter/driven/blockstore/flashstore/... ./adapter/driven/blockstore/conformance/... \
  | grep -v '^github.com/tcw/ibsen'
```

(`errore` and `utils` are stdlib-only and reachable from the pure packages, which is why only `github.com/tcw/ibsen` paths are filtered; anything impure they reached would still show up, since `-deps` is transitive.)

The script checks two more rules, because purity alone does not make the dependencies point
inward:

2. No package under `core/` may import `adapter/` or `wiring/`. A port that names one of its
   adapters is a port nothing else can implement.
3. No driving adapter may import a driven adapter directly, which would route around the
   hexagon. Test-support packages are exempt, since a test composes its own adapters. There
   are no other exceptions.

Everything below hangs off that rule: the bugs are the core earning trust, the ports are the discipline, compression/dictionaries/fencing/embedded builds are adapters and build-time choices behind it, and the migration is how we get there without breaking what works.

## What it is

A Go append-only log server, Kafka-like: topics you write entries to and read back by offset, over gRPC, with a sparse index and block-based storage on afero.

- Entry wire format (`core/domain/fsUtils.go` `CreateByteEntry`): `crc32c(4) | size uint64 LE (8) | entry | offset uint64 LE (8)`; CRC covers size, entry, offset.
- Blocks are named by the offset of their first entry, and carry a log kind and an index kind. Index block = pairs of `(offset uint64, byteOffset uint64)`. Where those bytes live is the adapter's business; the afero one keeps them at `<root>/<topic>/%020d.log` and `.idx`.
### Layout

The tree names which side of the hexagon everything is on. Dependencies point inward only:
an adapter may import `core/`, and `core/` may import nothing but `core/` (plus `errore` and
`utils`, which are stdlib-only).

```
core/                       the hexagon
  domain/                   Offset, TopicName, LogEntry, entry wire codec, topic-name rules
  port/
    driver/                 inbound: LogManager, ReadParams
    driven/                 outbound: BlockStore, Syncable, SingleIbsenWriterLock
  topic/                    the Topic aggregate and its recovery
  index/                    sparse index
  logfmt/                   entry framing, block scanning, RecoverBlock
  manager/                  application service; implements driver.LogManager

adapter/
  driver/                   things that drive the core
    grpcapi/                gRPC server
    cli/                    cobra CLI
  driven/                   things the core drives
    blockstore/{aferostore,memstore,flashstore,faultfs,conformance}
    locking/                file-lease adapter for driven.SingleIbsenWriterLock
    logging/zerologger/     zerolog adapter for driven.Logger
    telemetry/              OTEL

wiring/                     composition root: builds adapters, owns lifecycle
main.go                     entry point
errore/ utils/              stdlib-only, shared by both sides
```

- Pure today: all of `core/`, plus the `memstore`, `flashstore` and `conformance` packages under `adapter/driven/blockstore`, plus `errore` and `utils`. The core reaches nothing outside the standard library, and nothing outside `core/`.
- Not pure, by design: everything under `adapter/` and `wiring/`. `adapter/driven/locking` imports `uuid` and `afero`; `adapter/driven/logging/zerologger` imports `zerolog`; the driving adapters import gRPC and cobra.

## Current baseline (verified 2026-09-17, go1.26.4)

- `go test -race ./...` passes through migration step 5. Run it before and after every migration step.
- Port conformance suite: `adapter/driven/blockstore/conformance`, run by every adapter (`aferostore` on an in-memory filesystem and on a real directory, `memstore`, `flashstore`).
- Core property tests: `core/topic/topicAccess_property_test.go` (read-from-every-offset across block sizes and reload modes; concurrent write/read/index), run against every adapter. Only `coreBackends()` at the top of that file knows which store is behind the port.
- Crash and torn-write fault injection: `adapter/driven/blockstore/faultfs` tears a write at a chosen byte and fails everything after it. Used by `adapter/driven/blockstore/aferostore/crash_test.go` and `core/topic/topicAccess_crash_test.go`, on an in-memory filesystem and on a real directory.
- `Topic` state is guarded by `Topic.mu`; `Read` works on a `snapshot()` so slow consumers never block writers.
- `go vet ./...` is clean; keep it that way.
- CI (`.github/workflows/ci.yml`) runs gofmt, `go vet`, `scripts/check-architecture.sh` and `go test -race ./...` on every push. It takes its Go version from the `go` directive in `go.mod`, which is `1.26.4`.
- Dependencies are current as of 2026-09-17 (OTEL 1.46, gRPC 1.84, zerolog 1.35, cobra 1.10, afero 1.15). No deprecated gRPC dialling left: `grpcapi.DialContext` wraps `grpc.NewClient` and waits for the connection the way `grpc.WithBlock` used to, since `NewClient` connects lazily and would otherwise hand back a healthy-looking client for a server that is not there. Every client — CLI, bench and test helpers — goes through it, and `adapter/driver/grpcapi/client_test.go` pins that an unreachable address is an error rather than a client. CLI commands now bound that wait with `connectTimeout` (10s); `grpc.Dial` with `WithBlock` was given no context, so an unreachable server hung the command.
- `Start` and `shutdown` can run on different goroutines, so what `Start` builds is guarded: `IbsenServer.mu` covers `topicsManager`, `grpcServer` and the lifecycle channels (`lifecycle()` makes the pair once), and `grpcapi.IbsenGrpcServer` guards its `*grpc.Server` behind `Stop`/`GracefulStop`, which are safe before `StartGRPC` has created it and record the request so it is honoured.
- `Start` returns its failures instead of exiting: a refused single-writer lock is `wiring.ErrWriteLockUnavailable`, matchable with `errors.Is`, so a program embedding the log decides what to do. The CLI reports it and exits.
- Composition: `wiring.IbsenServer` builds every adapter. `Lock` is an optional injection point — `defaults()` builds a `FileLock` at `<root>/.writeLock` when none is given, which `wiring/lock_test.go` pins — and the OTEL exporter's lifetime is held by `Start`, not by `grpcapi.StartGRPC`.
- Logging port: `adapter/driven/logging/zerologger` has its own tests (level mapping, every field kind, `Enabled` agreeing with what is emitted, nil error dropped); `core/topic/logging_test.go` proves the core reaches its logger only through the port.

## 1. Correctness bugs

All known bugs below are fixed (2026-09-15), each with a regression test. Remaining known gaps are listed at the end.

- **Entry decoding** is one function, `domain.ReadEntry`: it verifies the CRC, treats a size larger than `MaxEntrySize` (or than the remaining file) as corruption, and distinguishes `io.EOF` (clean boundary), `io.ErrUnexpectedEOF` (partial entry) and `domain.ErrCorruptEntry`. Index building, offset scans, reads and recovery all use it. This fixed the uint64/uint32 size mismatch, the CRC read from the wrong buffer, and unverified reads.
- **Recovery**: `logfmt.RecoverBlock` replaces `BlockInfo`. It scans the head block from the start, truncates from the first partial or corrupt entry, and errors (without truncating) on a valid entry with an unexpected offset. Complete entries of a batch whose write returned an error can survive recovery, as with any crash; there are no batch markers.
- **Index after recovery**: pairs past the recovered end and torn partial pairs are dropped from the head index; indexing resumes right after the last kept entry. `IndexPosition.ByteOffset` always means "end of the scanned region", and the builder indexes every entry with `offset % 10 == 0`, including a block's first.
- **Writes**: open failures return errors; a failed write truncates the block back to `HeadBlockSize`; if that also fails the topic refuses writes until `LoadOrCreate`. Failed index writes are rolled back; index file handles are closed.
- **Reads**: batch byte cap works; batch size 0 is an error; the batch buffer is no longer preallocated to a client-supplied size.
- **Topics**: reloading a topic directory with no blocks (created by a read of an unknown topic) is valid instead of a `log.Fatal`; a concurrent `Mkdir` of the same topic is not an error.
- **Concurrency and cross-block index lookups**: fixed in step zero (`Topic.mu`, read snapshot, index only used for its own block).
- **gRPC `Read`**: a failed `Send` or a departed client used to hang the handler and reader forever, and every empty poll while tailing leaked a goroutine. Reads now take a `Cancel` channel (`domain.ReadLogParams`, `driver.ReadParams`, `logfmt.ReadFileParams`); the handler sends from its own goroutine via `streamFrom`, cancels and drains on send failure, and watches the stream context while polling. Covered by `adapter/driver/grpcapi/api-server_test.go` (fake stream, no network).
- **Stray files**: topic loading only considers block names the topic writes (`%020d.log` / `.idx`) and ignores anything else with a warning; `ListAllTopics` only lists directories. The manager no longer `log.Fatal`s when a topic fails to load: the request gets the error, the topic is not cached, and other topics keep working.
- **Topic names**: `domain.ValidateTopicName` rejects names that would escape or misuse the topic directory: empty or longer than 255 bytes, a leading dot (covers `.` and `..`), `/`, `\`, or control characters. `Topic.LoadOrCreate`, `Write` and `Read` enforce it; the gRPC handlers check first and return `InvalidArgument`.
- **Concurrent first loads**: `LogTopicsManager.getOrCreateTopic` runs at most one load per topic. Requests that arrive during a load wait for it and share its result; a failed load is not cached, so the next request retries. Before, two first requests could both run `LoadOrCreate` and keep one, and the discarded load kept recovering the head block and rewriting its index while the kept topic accepted writes, which could truncate acknowledged entries. Covered by `core/manager/topicLoad_test.go` (gated fs, counts loads).
- **Unsent log events**: 27 zerolog events in production code had no `.Msg`/`.Send`, so they logged nothing and never exited (25 `log.Fatal().Err(err)`, a `log.Err(err)` for a failed listen, and the background index failure in `Topic.Write`). The CLI now logs and exits with a message; `wiring/ibsen.go` returns profiling setup errors from `Start` and only logs profiling errors during shutdown, so the lock is still released; the client constructors return the dial error. `logcalls_test.go` parses the repo and fails on any zerolog event that is never sent.
- **Single-writer lease renewal** (`adapter/driven/locking`): renewal opened the lock file with `O_RDWR|O_EXCL`, which Linux ignores but afero's in-memory fs rejects for an existing file; it now opens with `O_WRONLY|O_TRUNC`. A failed renewal exits the process (another instance could claim the lock). `ReleaseLock` stops the renewer under a mutex, so a clean shutdown no longer races a renewal of the removed file (which used to panic on a nil file). The expired-lease claim checks its write. Tested on mem and OS fs.
- **TLS paths**: `adapter/driver/grpcapi/api-server.go` used grpc's `testdata.Path` for the cert and key, which joins relative paths onto grpc's own test data directory. `serverCredentials` now loads them as given (relative to the working directory) and returns the error before using the credentials. Covered by `adapter/driver/grpcapi/tls_test.go` (self-signed cert at relative paths, TLS round trip).
- **Shutdown**: `LogTopicsManager.Close` refuses new writes and topic loads (`manager.ErrClosed`), waits for those in flight, stops the index scheduler and closes each topic; `Topic.Close` refuses writes (`topic.ErrTopicClosed`) and waits for the indexing earlier writes started. Loaded topics stay readable. `IbsenServer.ShutdownCleanly` closes the manager after gRPC stops (a forced stop does not wait for handlers) and only then releases the lock, and `Start` waits for the shutdown to finish, since the process exits when it returns. gRPC calls during shutdown get `Unavailable`. Only the Write goroutine adds to `Topic.indexWg`, under `Topic.mu` before `closed` is set, so `Close` never races an `Add`. Covered by `core/manager/close_test.go` (gated writes) and `wiring/ibsen_test.go`.
- **CLI clients**: `newIbsenClient` / `newIbsenBench` discarded the `context.WithTimeout` cancel (`go vet`); the clients now keep it and the connection, and each command defers `Close`.
- **End-to-end test harness** (`adapter/driver/grpcapi/test`): each test starts its own server on a free port with `startTestServer(t)`, which fails the test if the server does not start and stops it on cleanup; clients are closed. Before, `TestName` left its server running, the next server failed to bind silently (`log.Fatal().Err(err)` without `Msg` never logs or exits), and `-count=2` hung. `TestReadWriteWithOffsetVerification` is enabled and reads from every offset across several blocks. A simulated reader whose stream fails to open returns instead of calling `Recv` on a nil stream (that crashed `TestName` now and then).

Known, not yet fixed: none.

## 2. Durability

fsync-on-flush policy: flush after N entries or a time interval. Only acknowledge a write and advance the visible offset once its flush completes, so readers never see unflushed data. (Today `NextOffset` advances right after `file.Write`, with no sync.)

## 3. Index

- ~~Binary search over the already-sorted offsets instead of the linear scan.~~ `Index.FindNearestByteOffset` is a `sort.Search` for the first pair past the offset, returning the one before it. The pairs are appended in scan order, so they are already sorted; a zero pair still means "nothing at or before this, scan from the start of the block", which is reachable for a block that does not begin on a multiple of the sparsity. `core/index/find_test.go` holds the scan it replaced and asserts the two agree for every query across seven index shapes.
- ~~Make sparsity configurable.~~ `topic.Params.IndexSparsity` (0 means `topic.DefaultIndexSparsity`, 10), threaded through `manager.LogTopicManagerParams` and `wiring.IbsenServer` to the CLI's `--indexSparsity`/`-i` and `IBSEN_INDEX_SPARSITY`. It is per-topic state, copied by `snapshot()`. Changing it between runs is safe and tested: the pairs already written stay valid and sorted, and the block ends up indexed at two densities. `index.CreateBinaryIndexFromLog` returns `index.ErrInvalidSparsity` for 0 rather than reaching `offset % 0`, which panics.
- Checksum index files.
- Dead code: `Index.addAll` and `Index.addIndex` are unexported with no callers.

## 4. Architecture: hexagonal refactor

The tree is hexagonal: `core/` is the hexagon, `adapter/driver/*` drives it, `adapter/driven/*`
is driven by it, `wiring/` assembles them. Storage, coordination and logging are ports, and
the core is pure.

- Pure core, stdlib only, ports in domain terms.
- Key port: narrow **`BlockStore`**. Block verbs: `List`, `Append`, `Open` (read at a byte offset), `Remove`. Plus `Truncate`, because crash recovery has to cut a torn tail, and `Topics` / `CreateTopic`, because a log server has to enumerate and create topics. Deliberately *not* a filesystem abstraction: no directories, handles, seeks or permissions.
- `Append` is all or nothing. A failed append leaves the block as it was; one that could not be rolled back wraps `driven.ErrDirtyBlock`, which is how the core learns a block must be recovered before it is appended to again.
- Optional **`Syncable`** capability, probed by type assertion through `driven.Sync`.
- Adapters, all under `adapter/driven/blockstore`: `aferostore` (filesystem, the one the server wires), `memstore` (pure in-memory), `flashstore` (a fixed region of raw flash: fixed pages, write-once bytes, page table in RAM).
- gRPC is a driving adapter; on embedded, skip it and call the log as a library.
- Coordination port: `driven.SingleIbsenWriterLock`, satisfied by `adapter/driven/locking` (file lease) and by `driven.NoFileLock` for a single-process or embedded deployment. The port names none of its adapters; each adapter asserts it satisfies the port.
- Driving port: `driver.LogManager` (`List`, `Write`, `Read`), implemented by `core/manager` and consumed by `adapter/driver/grpcapi`. A driving adapter names the port, never the implementation.
- Logging port: `driven.Logger`, two methods. `Log(level, msg, fields...)` takes typed `Field` values (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Err`), so an adapter switches on `FieldKind` exhaustively and never reaches for reflection; `Enabled(level)` lets the core skip building a payload that would be discarded. `driven.NopLogger` is the default when no adapter is wired, which is what lets an embedded build carry no logging code at all. Adapter: `adapter/driven/logging/zerologger`.

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
- Coordination is an optional adapter, no-op locally, so embedded doesn't carry it. (Existing `consensus/` + `adapter/driven/locking` is the seed.)

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

0. ~~Property tests green against today's code (including `-race`).~~
1. ~~Define the port as a thin afero wrapper.~~
2. ~~Route the core through the port while afero is still the only backend (the invasive change, against a trusted backend).~~
3. ~~Add the in-memory adapter to prove the port's shape.~~
4. ~~Add durability and crash tests inside the FS adapter.~~
5. ~~Add exotic embedded adapters last, validated by the shared suite.~~
6. ~~Restructure the tree into `core/` + `adapter/{driver,driven}` + `wiring/`, so the layout
   states the architecture instead of only the dependency graph implying it.~~
7. ~~Define the logging port and take `zerolog` out of the core.~~
8. ~~Move the lock and OTEL exporter construction into `wiring/`, so no driving adapter
   reaches a driven one.~~
9. ~~Return a refused write lock from `Start` instead of exiting the process.~~
10. ~~Guard what `Start` builds against a shutdown on another goroutine.~~
11. ~~Update every dependency, build with go 1.26.4, and move off the deprecated gRPC dialling.~~
12. ~~Binary search in the index instead of the scan back from the end.~~
13. ~~Make the index sparsity configurable instead of a constant.~~

Every step ships green. Steps 0 to 13 are done, one commit each.

Next, in the same one-change-at-a-time way: the durability flush policy (§2), the rest of the
index work (§3), and then compression (§5) and the embedded wiring files (§8). The flash adapter is the
proof the port is narrow enough; the embedded build still has to be wired and its dependency
graph checked.

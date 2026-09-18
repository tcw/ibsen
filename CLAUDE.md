# Ibsen

## North star

**Keep the core pure. It imports only the standard library, limited to the subset TinyGo supports (no `os`, `net`, or global logger), and speaks only in domain types. Storage, durability, compression, replication, transport, logging, and telemetry are all adapters behind ports, chosen at wiring time in `wiring/`. That one rule is what lets the same log core run on a microcontroller or in a replicated Kubernetes cluster without changing a line of it.**

Enforced by `scripts/check-architecture.sh`, which CI runs on every push. Its first rule is
this command; it must print nothing. All of `core/` is pure, so the whole hexagon is covered
by one pattern, with the embedded composition root and the three stdlib-only adapters named
beside it:

```sh
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' \
  ./core/... ./wiring/embedded/... \
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
- A log block is a sequence of **frames** (`core/domain/frame.go`), and a frame holds one
  write batch of those entries, put through a codec. Header, 36 bytes, little endian:
  `magic(4) | headerCrc(4) | codec(1) | version(1) | reserved(2) | firstOffset(8) | entryCount(4) | storedSize(4) | plainSize(4) | payloadCrc(4)`.
  The header CRC covers the 28 bytes after it, which includes the payload CRC, so no length
  read from a block is acted on before it has been checked. A frame can be placed, skipped
  and checked without being decoded, which is what lets recovery and indexing work without
  the codec the frame was written with.
- Blocks are named by the offset of their first entry, and carry a log kind and an index kind. Index block = checksummed pairs, `crc32c(4) | offset uint64 LE (8) | byteOffset uint64 LE (8)`, `index.PairSize` bytes each, the CRC covering the two values. Where those bytes live is the adapter's business; the afero one keeps them at `<root>/<topic>/%020d.log` and `.idx`.
### Layout

The tree names which side of the hexagon everything is on. Dependencies point inward only:
an adapter may import `core/`, and `core/` may import nothing but `core/` (plus `errore` and
`utils`, which are stdlib-only).

```
core/                       the hexagon
  domain/                   Offset, TopicName, LogEntry, entry wire codec, topic-name rules
  port/
    driver/                 inbound: LogManager, ReadParams
    driven/                 outbound: BlockStore, Syncable, SingleIbsenWriterLock, Codec, Logger
  topic/                    the Topic aggregate and its recovery
  index/                    sparse index
  logfmt/                   frame encode/decode, block scanning, RecoverBlock
  manager/                  application service; implements driver.LogManager

adapter/
  driver/                   things that drive the core
    grpcapi/                gRPC server
    cli/                    cobra CLI
  driven/                   things the core drives
    blockstore/{aferostore,memstore,flashstore,faultfs,conformance}
    compression/zstd/       zstd adapter for driven.Codec
    locking/                file-lease adapter for driven.SingleIbsenWriterLock
    logging/zerologger/     zerolog adapter for driven.Logger
    telemetry/              OTEL

wiring/                     composition root: builds adapters, owns lifecycle
  embedded/                 the other one: the log as a library, stdlib-only
    example/                the smallest embedded program, built to be weighed
main.go                     entry point
errore/ utils/              stdlib-only, shared by both sides
```

- Pure today: all of `core/`, all of `wiring/embedded/` including its example program, plus the `memstore`, `flashstore` and `conformance` packages under `adapter/driven/blockstore`, plus `errore` and `utils`. The core reaches nothing outside the standard library, and nothing outside `core/`.
- Not pure, by design: everything under `adapter/`, and `wiring/` itself.
- **afero is on its way out.** It was the storage port before `BlockStore` existed, and is now a second filesystem abstraction underneath our own. It costs 1.63 MB — a minimal build goes from 1.77 MB with `memstore` to 3.40 MB with `aferostore` — because `github.com/spf13/afero` imports `net/http` and `golang.org/x/text`, neither of which a log server needs to read a file. Its in-memory filesystem has also cost correctness twice, both times by behaving unlike a real one: the `O_RDWR|O_EXCL` renewal bug in §1, and `MemMapFs.OpenFile` checking and creating under separate locks so `O_CREATE|O_EXCL` is not atomic there. The removal is a strangler: (1) in-memory mode to `memstore`, done; (2) a `filestore` adapter on `os` behind a small owned seam — the adapter uses only `Exists`, `DirExists`, `Mkdir`, `MkdirAll`, `Open`, `OpenFile`, `ReadDir`, `Remove` and six methods on the handle — validated by the conformance, crash and property suites that already run against every adapter; (3) the same seam for `adapter/driven/locking`; (4) delete `aferostore` and the dependency. `adapter/driven/locking` imports `uuid` and `afero`; `adapter/driven/logging/zerologger` imports `zerolog`; `adapter/driven/compression/zstd` imports `klauspost/compress`; the driving adapters import gRPC and cobra.

## Current baseline (verified 2026-09-18, go1.26.4)

- `go test -race ./...` passes through migration step 17. Run it before and after every migration step.
- Port conformance suite: `adapter/driven/blockstore/conformance`, run by every adapter (`aferostore` on an in-memory filesystem and on a real directory, `memstore`, `flashstore`).
- Core property tests: `core/topic/topicAccess_property_test.go` (read-from-every-offset across block sizes and reload modes; concurrent write/read/index), run against every adapter. Only `coreBackends()` at the top of that file knows which store is behind the port.
- Crash and torn-write fault injection: `adapter/driven/blockstore/faultfs` tears a write at a chosen byte and fails everything after it. Used by `adapter/driven/blockstore/aferostore/crash_test.go` and `core/topic/topicAccess_crash_test.go`, on an in-memory filesystem and on a real directory. Nothing writes to the log while a crash is armed for the index: a write waits on a flush of its log block, and a crash tripped by the background indexer fails that flush too, so the write fails for a reason the test is not about. `TestTopic_CrashDuringIndexWriteDropsTheTornPair` lags the index by truncating it and reloading instead, which is the same state without the race.
- `Topic` state is guarded by `Topic.mu`; `Read` works on a `snapshot()` so slow consumers never block writers.
- `go vet ./...` is clean; keep it that way. `staticcheck -checks U1000 ./...` prints nothing, so there is no unused code; CI does not run it.
- CI (`.github/workflows/ci.yml`) runs gofmt, `go vet`, `scripts/check-architecture.sh`, `scripts/embedded-size.sh` and `go test -race ./...` on every push. It takes its Go version from the `go` directive in `go.mod`, which is `1.26.4`.
- Dependencies are current as of 2026-09-18 (OTEL 1.46, gRPC 1.84, zerolog 1.35, cobra 1.10, afero 1.15, klauspost/compress 1.20). No deprecated gRPC dialling left: `grpcapi.DialContext` wraps `grpc.NewClient` and waits for the connection the way `grpc.WithBlock` used to, since `NewClient` connects lazily and would otherwise hand back a healthy-looking client for a server that is not there. Every client — CLI, bench and test helpers — goes through it, and `adapter/driver/grpcapi/client_test.go` pins that an unreachable address is an error rather than a client. CLI commands now bound that wait with `connectTimeout` (10s); `grpc.Dial` with `WithBlock` was given no context, so an unreachable server hung the command.
- `Start` and `shutdown` can run on different goroutines, so what `Start` builds is guarded: `IbsenServer.mu` covers `topicsManager`, `grpcServer` and the lifecycle channels (`lifecycle()` makes the pair once), and `grpcapi.IbsenGrpcServer` guards its `*grpc.Server` behind `Stop`/`GracefulStop`, which are safe before `StartGRPC` has created it and record the request so it is honoured.
- `Start` returns its failures instead of exiting: a refused single-writer lock is `wiring.ErrWriteLockUnavailable`, matchable with `errors.Is`, so a program embedding the log decides what to do. The CLI reports it and exits.
- In-memory mode (`--rootDirectory` unset) wires `memstore`, not the filesystem adapter over an emulated filesystem, so it reaches no filesystem at all and takes no write lock: the log lives in the process and is shared with nobody. `IbsenServer.Afs` may be nil in that mode. Pinned by `wiring/ibsen_test.go`, which writes through a running in-memory server and then walks the filesystem it was given to check nothing landed on it.
- Composition: `wiring.IbsenServer` builds every adapter. `Lock` is an optional injection point — `defaults()` builds a `FileLock` at `<root>/.writeLock` when none is given, which `wiring/lock_test.go` pins — and the OTEL exporter's lifetime is held by `Start`, not by `grpcapi.StartGRPC`.
- Index checksums: `core/index/checksum_test.go` covers the round trip, every byte of a pair being covered by its CRC, parsing stopping at a corrupt pair, torn trailing pairs, and the old format being rejected; `core/topic/indexChecksum_test.go` shows a corrupted pair and an unchecksummed block both being rebuilt into exactly what a clean scan of the log gives, with every offset still readable.
- Durability: `core/topic/flush_test.go` drives a `Syncable` store whose `Sync` the test gates, and covers the guarantee itself (a reader sees nothing until the flush returns, and `Write` does not return either), a failed flush being reported and leaving nothing readable, those entries appearing once a later flush succeeds, concurrent writers sharing one sync, the interval releasing a writer that never reaches the threshold, a non-syncable store never waiting, and a reloaded topic counting its recovered block as durable.
- Logging port: `adapter/driven/logging/zerologger` has its own tests (level mapping, every field kind, `Enabled` agreeing with what is emitted, nil error dropped); `core/topic/logging_test.go` proves the core reaches its logger only through the port.
- Framing: `core/domain/frame_test.go` covers the header round trip, every one of its 36
  bytes being caught by its checksum, a missing magic, a future version, a stored size larger
  than what is left of the block, and read/verify/skip of a payload agreeing with each other
  across the streaming buffer boundary. `core/logfmt/logUtils_test.go` covers recovery of a
  partial header, a partial payload, a garbage tail and a corrupt frame, a pre-framing block
  being reported rather than truncated, recovery of a frame whose codec is not wired, and a
  read rejecting a corrupt frame, a corrupt entry inside a valid frame, and an unwired codec.
  `core/topic/frame_test.go` covers the frame bounds, a write reaching the store as one
  append however many frames it makes, and one block holding frames of two codecs.
- Indexing coalesces instead of dropping work: `Topic.UpdateIndex` leaves a mark when it finds a run under way, and the run takes that mark before it stops, so the tail of a log is always indexed by somebody. There is no background sweep and no ticker. `core/topic/indexing_test.go` gates the store to hold a run still, proves a request arriving during it is picked up rather than dropped, covers sixteen callers coalescing into one run, and covers a failed run leaving the work for the next call. `core/manager/topicsManager_test.go` pins that building a manager starts no goroutine and that the index is complete once writes stop.
- Embedded: `wiring/embedded` is the second composition root, and `wiring/embedded/embedded_test.go` covers a store being required, write/list/read with nothing else wired, the log satisfying `driver.LogManager`, the block-size default, every other param reaching the core untouched, and `Close` refusing writes while loaded topics stay readable.
- Frame bounds reach a topic through the manager, and zero still means the topic defaults:
  `core/manager/topicsManager_test.go`. `adapter/driver/cli/root_test.go` covers the flag
  validation, including the largest frame the format allows and the first one past it.
- `core/port/driven/codec_test.go` covers the registry: the identity codec always present, an
  unwired codec named in the error, and a nil registry still reading uncompressed frames.
- Compression: `adapter/driven/compression/zstd` has round trips at every level, the append
  and no-retain contracts the port states, an unknown level refused, one codec under
  concurrent frames, and a frame too large to hold refused before it is decoded. Its
  `topic_test.go` drives a real `Topic` through it: a log written with zstd is smaller (500
  JSON-shaped entries: 42926 bytes plain, 3945 zstd) and every offset still reads back, a
  zstd topic reloads, a block survives the codec being turned on and off again, and a build
  carrying no zstd reads the uncompressed frames of a mixed block, loads and recovers it, and
  names the missing codec for the rest. `wiring/lock_test.go` pins that a compression name
  picks the codec, that an unknown name or level is refused, that every linked codec is in
  the read registry whatever the server writes with, and that an injected codec is kept.

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
- **Single-writer lease renewal** (`adapter/driven/locking`): renewal opened the lock file with `O_RDWR|O_EXCL`, which Linux ignores but afero's in-memory fs rejects for an existing file; it now opens with `O_WRONLY|O_TRUNC`. `ReleaseLock` stops the renewer under a mutex, so a clean shutdown no longer races a renewal of the removed file (which used to panic on a nil file). The expired-lease claim checks its write. Tested on mem and OS fs.
- **Two instances could both claim the same lock.** A free lock was claimed with `O_CREATE` and no `O_EXCL`, which is an open rather than a claim, so two servers starting together both created the file and both came away holding the lease; it is now `O_CREATE|O_EXCL`, the one step a filesystem makes atomic. An expired lease has no such primitive — there is no compare-and-swap — so a takeover is confirmed rather than assumed: the claimant pauses for a tenth of the lease and reads the file back, and only the instance whose id is in it carries on. Every claim is written to a temporary file and renamed into place, so a reader sees the old holder or the new one and never a half-written name; the truncate-then-write it replaced produced real short reads under contention, which a holder renewing its own lease would have read as having lost it. The adapter needs a filesystem where `O_CREATE|O_EXCL` and `Rename` are atomic: afero's MemMapFs is not one, since its `OpenFile` checks and creates under separate locks, which is why the two race tests run only against a real directory. The server takes this lock only when it is not running in memory, so that is not a deployment.
- **A stalled writer used to steal its own lease back, and two writers then shared one log.** Renewal truncated the lock file and wrote its own id without reading it first. So: the holder stalls past its lease (a garbage collection pause, a frozen scheduler, a slow disk), the lease expires, a second instance legitimately claims it, the first wakes up and overwrites the file back to its own id — and both then believe they hold the lease. No partition needed, one long pause on one machine. Renewal now proves the lease is still its own before extending it, and treats its own lease having aged out as lost even if nobody has taken it, since anyone may take it at any moment. `ReleaseLock` follows the same rule and will not remove a lock file it no longer holds, which would hand the log to a third writer. Losing the lease is reported through `locking.LeaseLost` rather than exited from inside the adapter: `wiring.IbsenServer.writeLockLost` is what stops the process, and it exits rather than shutting down cleanly, because a clean shutdown flushes and writes. A filesystem has no compare-and-swap, so renewal is a read and then a write: this closes the window a stall opens, it does not make renewal atomic. `adapter/driven/locking/singleInstanceLock_test.go` reproduces the theft on both filesystems and pins all four rules.
- **TLS paths**: `adapter/driver/grpcapi/api-server.go` used grpc's `testdata.Path` for the cert and key, which joins relative paths onto grpc's own test data directory. `serverCredentials` now loads them as given (relative to the working directory) and returns the error before using the credentials. Covered by `adapter/driver/grpcapi/tls_test.go` (self-signed cert at relative paths, TLS round trip).
- **Shutdown**: `LogTopicsManager.Close` refuses new writes and topic loads (`manager.ErrClosed`), waits for those in flight, and closes each topic; `Topic.Close` refuses writes (`topic.ErrTopicClosed`) and waits for the indexing earlier writes started. Loaded topics stay readable. `IbsenServer.ShutdownCleanly` closes the manager after gRPC stops (a forced stop does not wait for handlers) and only then releases the lock, and `Start` waits for the shutdown to finish, since the process exits when it returns. gRPC calls during shutdown get `Unavailable`. Only the Write goroutine adds to `Topic.indexWg`, under `Topic.mu` before `closed` is set, so `Close` never races an `Add`. Covered by `core/manager/close_test.go` (gated writes) and `wiring/ibsen_test.go`.
- **CLI clients**: `newIbsenClient` / `newIbsenBench` discarded the `context.WithTimeout` cancel (`go vet`); the clients now keep it and the connection, and each command defers `Close`.
- **CLI read arguments**: `client read <topic> <offset> <batchSize>` parsed the batch size and left the offset at zero, so asking for the tail of a topic read all of it; two arguments worked, which is why it went unnoticed. The branches tested `len(args) == 2` and `== 3` rather than `>=`. A failed parse also printed a line and carried on with the zero `strconv.ParseUint` returns, and the batch-size message named the offset argument. Parsing now lives in `parseReadArgs`, refuses what it cannot parse, and is covered by `adapter/driver/cli/root_test.go`, the package's first test.
- **End-to-end test harness** (`adapter/driver/grpcapi/test`): each test starts its own server on a free port with `startTestServer(t)`, which fails the test if the server does not start and stops it on cleanup; clients are closed. Before, `TestName` left its server running, the next server failed to bind silently (`log.Fatal().Err(err)` without `Msg` never logs or exits), and `-count=2` hung. `TestReadWriteWithOffsetVerification` is enabled and reads from every offset across several blocks. A simulated reader whose stream fails to open returns instead of calling `Recv` on a nil stream (that crashed `TestName` now and then).

Known, not yet fixed: none.

## 2. Durability

Done. A write is acknowledged, and its offsets become readable, only once the flush covering
them has returned, so a reader never sees an entry a power cut could take back.

- `NextOffset` is still the next offset to assign. The read boundary is now the *durable*
  offset, which trails it by whatever is appended but not yet flushed:
  `endBoundaryForReadOffset` returns that, and `snapshot()` carries the flusher so a read is
  bounded by it.
- Policy: `Params.FlushEntries` (how many entries may wait; 0 means `DefaultFlushEntries`,
  which is 1, so every write is flushed before it is acknowledged) and `Params.FlushInterval`
  (how long a batch may be held back hoping for more; 0 never holds one back). Threaded
  through the manager params and `wiring.IbsenServer` to `--flushEntries`/`-f`,
  `--flushIntervalMs`, `IBSEN_FLUSH_ENTRIES` and `IBSEN_FLUSH_INTERVAL_MS`.
- **No background goroutine.** The writer that needs its entries durable drives the flush;
  writers whose entries joined the same batch wait on it. The core starts no timers it does
  not own and no goroutine outlives a topic. Once driving, a driver takes each batch as it
  finds it rather than re-applying the policy: the next batch formed while the previous one
  was syncing, so it has already waited.
- A store that is not `Syncable` has nothing to push, so its entries are durable when
  `Append` returns and none of the waiting applies. That is why `memstore` and `flashstore`
  pay nothing for this.
- A failed flush is returned to every writer waiting on it, leaves the durable offset where
  it was, and puts its blocks back into the next batch. The entries are written but their
  durability is unknown, so nothing may read them as committed; a later flush covers them and
  then they appear. A failure does not retry inside the same driver, which would spin.
  `Topic.Close` makes one last attempt at whatever a failed flush left behind.
- **Index blocks are deliberately not flushed.** The index is derivable from the log, and
  recovery already drops torn pairs and re-indexes, so paying an fsync for it would buy
  nothing.

## 3. Index

- ~~Binary search over the already-sorted offsets instead of the linear scan.~~ `Index.FindNearestByteOffset` is a `sort.Search` for the first pair past the offset, returning the one before it. The pairs are appended in scan order, so they are already sorted; a zero pair still means "nothing at or before this, scan from the start of the block", which is reachable for a block that does not begin on a multiple of the sparsity. `core/index/find_test.go` holds the scan it replaced and asserts the two agree for every query across seven index shapes.
- ~~Make sparsity configurable.~~ `topic.Params.IndexSparsity` (0 means `topic.DefaultIndexSparsity`, 10), threaded through `manager.LogTopicManagerParams` and `wiring.IbsenServer` to the CLI's `--indexSparsity`/`-i` and `IBSEN_INDEX_SPARSITY`. It is per-topic state, copied by `snapshot()`. Changing it between runs is safe and tested: the pairs already written stay valid and sorted, and the block ends up indexed at two densities. `index.CreateBinaryIndexFromLog` returns `index.ErrInvalidSparsity` for 0 rather than reaching `offset % 0`, which panics.
- ~~Checksum index files.~~ Each pair carries a crc32c over its two values, so a pair either verifies or is not there, the same rule a log entry follows. `NewIndex` stops at the first pair that is torn or fails its checksum and returns the good prefix, which the existing truncation drops the rest of and rebuilds from the log. Without this a corrupt pair pointed at a byte that is not an entry boundary and a read from it failed on EOF; the test for it fails that way when the check is removed.
- The format change needs no migration: an index written as bare 16-byte pairs fails at its first pair and is rebuilt whole. The index says nothing the log does not.
- Since framing (§5), a pair points at the start of a **frame**, never inside one: a frame is
  decoded whole, so there is nothing finer to aim at. A frame earns a pair when it covers an
  offset that is a multiple of the sparsity, which is the same set of pairs the entry-wise
  rule gave when every entry had a frame of its own, and one pair per frame once frames are
  larger than the sparsity. A read therefore scans at most one frame plus the sparsity.

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
- Coordination port: `driven.SingleIbsenWriterLock`, satisfied by `adapter/driven/locking` (file lease, which self-fences the moment it cannot prove it still holds the lease) and by `driven.NoFileLock` for a single-process or embedded deployment. The port names none of its adapters; each adapter asserts it satisfies the port.
- Driving port: `driver.LogManager` (`List`, `Write`, `Read`), implemented by `core/manager` and consumed by `adapter/driver/grpcapi`. A driving adapter names the port, never the implementation.
- Compression port: `driven.Codec`, three methods (`ID`, `Encode`, `Decode`), named in a frame
  by one byte. `driven.Codecs` is the registry a read resolves that byte against, built at
  wiring time, which is what decides how much compression code a binary links. `driven.NoCodec`
  is the identity codec and the only one in the core, so a build that wires no compression
  adapter still writes and reads frames. A codec may never change what it produces for an id:
  an id is a promise about bytes already on disk.
- Logging port: `driven.Logger`, two methods. `Log(level, msg, fields...)` takes typed `Field` values (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Err`), so an adapter switches on `FieldKind` exhaustively and never reaches for reflection; `Enabled(level)` lets the core skip building a payload that would be discarded. `driven.NopLogger` is the default when no adapter is wired, which is what lets an embedded build carry no logging code at all. Adapter: `adapter/driven/logging/zerologger`.

## 5. Compression

Framing is done (step 17); the codec adapter behind it is not (step 18). Today every frame is
written with `driven.NoCodec`, so the format is in place and carries no compression yet.

- ~~Per-block compressed **frames**, self-describing: header carries codec, offsets, two
  CRCs.~~ The layout is under "What it is". Self-describing is the point: a frame says which
  codec wrote it, so a block may hold frames of several codecs and changing the codec never
  rewrites anything. It also says where it sits and how big it is, so it can be placed,
  skipped and checked without being decoded — which is why recovery and indexing work on a
  block whose codec this build does not carry.
- ~~Index points at frame boundaries; reads decompress one frame and scan within it.~~ See §3.
  A read seeks to the frame holding the offset, decodes that one frame, and drops the entries
  in front of the offset asked for.
- ~~**Frame boundary = flush boundary = durability boundary.**~~ One `Write` is appended in one
  call, whole or not at all, so the store never holds half a frame and a flush never lands
  inside one. A large write becomes several frames in that one append: a frame is bounded by
  `Params.MaxFrameEntries` (1000) and `Params.MaxFrameBytes` (1 MiB), because a frame is
  decoded whole and the index can offer only one pair for it. Both are threaded through the
  manager params and `wiring.IbsenServer` to `--maxFrameEntries`, `--maxFrameBytes`,
  `IBSEN_MAX_FRAME_ENTRIES` and `IBSEN_MAX_FRAME_BYTES`. They are the dial between how well a
  codec can compress, which wants large frames, and how little a read has to decode to reach
  one offset, which wants small ones; the defaults are not measured, so they are a starting
  point rather than an answer.
- The entry checksum still earns its place inside a frame. The frame checksum catches the
  media; the entry checksum catches everything after it, and a frame that verifies whole can
  still hold an entry that does not.
- A block written before framing is **not** readable: `RecoverBlock` reports
  `domain.ErrUnsupportedLogFormat` and truncates nothing, since nothing says those bytes are
  damaged. That is a deliberate clean break — the log is not derivable the way the index is,
  so the frame header carries a magic purely to tell "written before framing" from "corrupt".
- The adapter is `adapter/driven/compression/zstd` (step 18), built by `wiring` rather than by
  the CLI, since a driving adapter must not reach a driven one: the CLI passes a name
  (`--compression` / `IBSEN_COMPRESSION`, `--compressionLevel` /
  `IBSEN_COMPRESSION_LEVEL`) and the composition root decides what it is made of. A name with
  no adapter behind it is `wiring.ErrUnknownCompression`, not a quiet fallback to none.
  `wiring.IbsenServer.Codec`/`Codecs` remain the injection points underneath, for a program
  that embeds the log and brings its own.
- **Compression chooses only what is written.** The read registry holds every codec the binary
  links, so turning compression off, or changing it, never strands a block written under the
  old setting. The level is not part of the format either: a frame says only that zstd wrote
  it.
- The default is `none`. Nothing about an existing deployment changes until someone asks for
  it.
- `cli.validateFrameBounds` refuses a frame the format could not hold: without it, a
  `maxFrameBytes` above `domain.MaxFrameSize` would start a server that fails on the first
  write large enough to reach the bound, rather than not starting.
- **What the frame bound costs**, measured by `adapter/driven/compression/zstd/bench_test.go`
  (`go test ./adapter/driven/compression/zstd/ -run XXX -bench BenchmarkFrame`). It varies
  `MaxFrameEntries` with `MaxFrameBytes` wide open and one write of 10000 entries, so the
  entry bound is the only thing deciding frame size. Storage is `memstore`, which cannot
  sync, so the flush policy stays out of the numbers. For ~130-byte JSON events through zstd
  at the default level, on a 2-core AMD Ryzen 5 2600X:

  | entries/frame | ratio | decoded per single-entry read | single read | sequential read |
  |---|---|---|---|---|
  | 1 | 1.36 | 410 B | 69 µs | 122 MB/s |
  | 10 | 0.30 | 1.6 KB | 96 µs | 121 MB/s |
  | 100 | 0.17 | 14 KB | 64 µs | 215 MB/s |
  | 1000 (default) | 0.16 | 137 KB | 248 µs | 403 MB/s |
  | 10000 | 0.17 | 1.4 MB | 2537 µs | 428 MB/s |

  The ratio is flat above 100: 1000 buys 4% over 100, and 10000 buys nothing. Read
  amplification is linear in the bound, because a frame is decoded whole — at the default a
  read of one 130-byte entry decodes 137 KB. Sequential reads want the opposite and flatten
  around 1000. So the bound is a choice between random and sequential readers, and the ratio
  stops arguing for large frames well before either of them does. A frame per entry is where
  a codec has nothing to offer: zstd on 130 bytes comes back larger, so every such frame falls
  back to the plain bytes and the 1.26 above is the framing overhead alone, the same as no
  codec at all. Before the fallback it was 1.36, meaning compression actively cost bytes. The
  index also gets a pair only every sparsity frames, so a small frame costs header scanning
  too.
- The CPU of a compression attempt that is then discarded is still paid; skipping it below a
  size threshold would save that, and wants its own measurement.
- Still to do: the default of 1000 entries per frame was chosen before any of this was
  measured, and the table says 100 is the better all-round answer. Changing it is a
  behaviour change for every deployment, so it is a decision, not a follow-up.

## 6. Dictionaries

- Per-topic trained zstd dictionaries, versioned by `dictID` in the frame header; immutable; retained while any frame references them.
- Cold start writes plain frames while reservoir-sampling entries; a background trainer builds the dictionary and flips a current pointer.
- Retention now couples dictionary lifetime to log lifetime.

## 7. Distribution

- Don't build Raft. Run on replicated storage as a Kubernetes StatefulSet.
- Keep single-writer safety with a fencing lease and monotonic token (likely etcd lease + mod-revision); storage rejects stale-token writes.
- Coordination is an optional adapter, no-op locally, so embedded doesn't carry it. `adapter/driven/locking` is the seed; there is no `consensus/` package, the step-6 restructure removed it.

What is already here, and what a token would actually add:

- The file lease is the "acquire, renew, self-fence" half, and it is honest about the window
  it closes. It stops the moment it cannot prove it holds the lease, including when its own
  lease has aged out with nobody having taken it.
- **A token cannot reach the storage without widening a port that is deliberately narrow.**
  `SingleIbsenWriterLock.AcquireLock() bool` has no token and `BlockStore.Append(ref, data)`
  has no token; threading one through would make memstore, flashstore and every
  microcontroller store carry a concept one deployment needs. It does not have to: a fencing
  store is a `BlockStore` decorator built in `wiring`, which checks the token on every append
  rather than once at startup. No core change, no port change.
- **Any `BlockStore` decorator must forward `Syncable`.** It is probed by type assertion in
  `driven.Sync` and in `newFlusher`, so a wrapper that does not implement it makes both
  conclude the store has nothing to flush, and every write becomes readable the moment
  `Append` returns. Nothing fails; the durability guarantee of §2 just quietly stops holding.
- Storage-side enforcement, which is what makes a token airtight rather than merely useful, is
  **not reachable through afero**: a filesystem has no conditional write. Getting it means an
  object store with `If-Match`, and object stores do not append, while `BlockStore` is built
  around appending to a growing block. That is a storage redesign, not a feature.
- The deployment this section describes already fences below Ibsen: a StatefulSet gives at
  most one pod per ordinal, and a ReadWriteOnce volume is attached to one node by the CSI
  driver. That has known edges around force-detach and unreachable nodes, which is a reason to
  keep the file lease as a cheap backstop rather than to build etcd integration.
- Claiming is as atomic as a filesystem allows: `O_CREATE|O_EXCL` for a free lock, which is
  exact, and confirm-after-a-pause for an expired one, which is not. What makes the second
  safe in the end is renewal, which gives up the moment it cannot prove the lease is still its
  own, so a claim that slips through is dropped within one renewal interval rather than
  running beside another writer for the life of the process. A token enforced by storage would
  replace that argument with a guarantee; nothing short of it removes the pause.

## 8. Embedded builds

Done. `wiring/embedded` is the second composition root: `Open(Params{Store: ...})` returns a
`*Log` that satisfies `driver.LogManager`, so an embedded program drives the log through the
same port a gRPC server does.

- ~~Build tags select wiring files, but the real lever is import discipline.~~ There are no
  build tags. The two composition roots are two **packages**, which is additive in a stronger
  sense than a tag: nothing is excluded, a program imports the root it wants, and the default
  build is still the full server. It is also the reason the rest of this works — a tag can
  only be trusted, while `go list -deps` on a package can be read.
- ~~Prefer two additive wiring files over exclusions.~~ `wiring/embedded` imports nothing but
  `core/`, so it is stdlib-only and sits in rule 1 of `scripts/check-architecture.sh`
  alongside the core. An embedded build links no gRPC, no cobra, no OTEL, no zerolog, no
  afero and no compressor, and CI fails if that stops being true.
- **What a build pays for is what it brings.** The store is injected, not chosen: a program
  bringing `memstore` or `flashstore` stays inside the standard library, one bringing
  `aferostore` pays for afero, one wiring the zstd codec pays for zstd. The same goes for the
  logger and the codec, which default to `NopLogger` and `NoCodec`.
- ~~`CGO_ENABLED=0`, `-ldflags="-s -w"`, cross-compile via `GOOS`/`GOARCH`.~~ Those are the
  flags `scripts/embedded-size.sh` builds with, and `GOOS`/`GOARCH` pass through it.
- ~~Verify by diffing binary size and inspecting the dependency graph, not by trusting the
  tags.~~ Both, and both in CI. The dependency graph is the gate, since it is exact. The size
  is the other side of the same claim — what the linker produced rather than what the imports
  promised — and the script fails if an embedded build stops being smaller than the server.
  A byte ceiling would only drift with each Go release.

Measured by `scripts/embedded-size.sh` on go1.26.4:

| target | server | embedded | |
|---|---|---|---|
| linux/amd64 | 16.48 MB | 1.87 MB | 8.8× smaller |
| linux/arm | 15.44 MB | 1.81 MB | 8.5× smaller |

- Nothing is woken on a timer. `manager.NewLogTopicsManager` used to start a goroutine with a
  ten-second ticker sweeping every loaded topic, to catch indexing its own exclusion flag had
  dropped; the topic now takes that work itself, so an embedded build carries no timer it did
  not ask for. The core starts no goroutine that outlives the call which made it: the flusher
  is driven by the writer that needs it (§2), and indexing by the write that dirtied the
  index.

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
14. ~~Acknowledge a write only once it is on durable media (§2).~~
15. ~~Checksum index pairs (§3).~~
16. ~~Remove the dead code the earlier steps left behind.~~
17. ~~Frame the log block and define the compression port, with the identity codec as the
    only one (§5). The invasive change, made against a codec that cannot lose data.~~
18. ~~Add the zstd adapter behind the codec port, built in `wiring` and chosen by name on the
    CLI (§5).~~
19. ~~Keep compression only where it paid, so turning it on cannot make a topic larger (§5).~~
20. ~~Add the embedded composition root and check it, by dependency graph and by binary size
    (§8).~~
21. ~~Coalesce indexing instead of dropping it, and delete the ten-second sweep that covered
    for the drop (§8).~~
22. ~~Stop a stalled writer from stealing its own lease back (§1, §7).~~
23. ~~Make claiming the lock as atomic as a filesystem allows, and stop a claim being read
    half-written (§1, §7).~~
24. ~~Wire in-memory mode to `memstore` instead of the filesystem adapter over an emulated
    filesystem, the first step of removing afero.~~

Every step ships green. Steps 0 to 24 are done, one commit each.

Next, in the same one-change-at-a-time way: the frame-bound default, which the benchmark has
an answer for and nobody has decided (§5); then dictionaries (§6), which §5's measurements
argue are narrower than they look; and §7, which is untouched.

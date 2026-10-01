# Architecture

This document describes how Ibsen is built and why. [README.md](README.md) is the
introduction; this is the level below it. It assumes you have read what a log is and want to
know what the bytes look like, what the interfaces promise, and which decisions are load
bearing.

- [1. The one rule](#1-the-one-rule)
- [2. The shape](#2-the-shape)
- [3. Package layout](#3-package-layout)
- [4. Domain model](#4-domain-model)
- [5. Storage formats](#5-storage-formats)
- [6. Ports](#6-ports)
- [7. Adapters](#7-adapters)
- [8. The topic aggregate](#8-the-topic-aggregate)
- [9. Durability](#9-durability)
- [10. The index](#10-the-index)
- [11. Recovery](#11-recovery)
- [12. Indexing without timers](#12-indexing-without-timers)
- [13. Compression](#13-compression)
- [14. Composition roots](#14-composition-roots)
- [15. Concurrency and shutdown](#15-concurrency-and-shutdown)
- [16. Single writer, and distribution](#16-single-writer-and-distribution)
- [17. Testing](#17-testing)
- [18. Extending Ibsen](#18-extending-ibsen)
- [19. Invariants](#19-invariants)

---

## 1. The one rule

> **The core is pure.** It imports only the standard library, limited to the subset TinyGo
> supports — no `os`, no `net`, no global logger — and speaks only in domain types. Storage,
> durability, compression, replication, transport, logging and telemetry are adapters behind
> ports, chosen at wiring time.

Everything else in this document follows from that. It is what lets the same log core run on
a microcontroller and in a replicated Kubernetes cluster without a line of it changing, and
it is the reason the architecture is checkable rather than merely intended.

It is enforced by [`scripts/check-architecture.sh`](scripts/check-architecture.sh), which CI
runs on every push. Three rules:

1. **Purity.** `go list -deps` over `core/...`, `wiring/embedded/...`, `adapter/driver/stdio`
   and the three standard-library storage adapters must name no non-standard package outside
   `github.com/tcw/ibsen`. Because `-deps` is transitive, anything impure reached indirectly
   shows up too.
2. **No package under `core/` imports `adapter/` or `wiring/`.** A port that names one of its
   adapters is a port nothing else can implement.
3. **No driving adapter imports a driven adapter.** That would route around the hexagon —
   the CLI choosing a compressor, say, instead of naming one and letting the composition root
   build it. Test-support packages are exempt, because a test composes its own adapters.
   There are no other exceptions.

A dependency graph is worth more here than a build tag, because a tag has to be trusted and a
graph can be read.

## 2. The shape

```
             driving adapters                                driven adapters
        (things that drive the log)                     (things the log drives)

        ┌──────────────┐                                  ┌────────────────────┐
        │  grpcapi     │──┐                            ┌─▶│ filestore          │
        ├──────────────┤  │                            │  │ memstore           │
        │  stdio       │──┤    ┌───────────────────┐   │  │ flashstore         │
        ├──────────────┤  ├───▶│ driver.LogManager │   │  │ faultfs (tests)    │
        │  cli         │──┤    └─────────┬─────────┘   │  └────────────────────┘
        └──────────────┘  │              │             │
        ┌──────────────┐  │        ┌─────▼─────┐       │  ┌────────────────────┐
        │ your program │──┘        │           │   ┌───┴─▶│ driven.BlockStore  │
        │  (embedded)  │           │   core/   │───┤      │ driven.Syncable    │
        └──────────────┘           │           │   ├─────▶│ driven.Codec       │ zstd
                                   │ manager   │   ├─────▶│ driven.Logger      │ zerologger
                                   │ topic     │   └─────▶│ SingleIbsenWriter  │ locking
                                   │ index     │          │        Lock        │ NoFileLock
                                   │ logfmt    │          └────────────────────┘
                                   │ domain    │
                                   └───────────┘
                                         ▲
                                         │ builds and injects everything
                                   ┌─────┴──────┐
                                   │  wiring/   │   wiring/embedded/
                                   └────────────┘
```

Dependencies point inward only. An adapter may import `core/`; `core/` imports nothing but
`core/`, plus `errore`, which is stdlib-only.

## 3. Package layout

```
core/                       the hexagon
  domain/                   Offset, TopicName, LogEntry, entry wire codec, frame header, name rules
  port/
    driver/                 inbound: LogManager, ReadParams
    driven/                 outbound: BlockStore, Syncable, SingleIbsenWriterLock, Codec, Logger
  topic/                    the Topic aggregate, its recovery, its flusher
  index/                    sparse index: build, parse, search, checksums
  logfmt/                   frame encode/decode, block scanning, RecoverBlock
  manager/                  application service; implements driver.LogManager

adapter/
  driver/
    grpcapi/                gRPC server and Go client
    stdio/                  the log over two byte streams, stdlib-only
    cli/                    cobra CLI
    history/                test support: records histories through the port and checks them
  driven/
    blockstore/{filestore,memstore,flashstore,faultfs,conformance}
                            faultfs: CrashFiles (torn writes), PageCache (power cuts)
    compression/zstd/       zstd behind driven.Codec
    locking/                file lease behind driven.SingleIbsenWriterLock
    logging/zerologger/     zerolog behind driven.Logger
    telemetry/              OpenTelemetry

wiring/                     composition root: builds adapters, owns lifecycle
  local.go                  OpenLocal: one process over a data directory
  embedded/                 the second one: the log as a library, stdlib-only
    example/                the smallest embedded program, built to be weighed
main.go                     entry point
errore/                     stdlib-only, shared by both sides
```

Pure today: all of `core/`, all of `wiring/embedded/` including its example, the `stdio`
driving adapter, the `filestore`, `memstore`, `flashstore` and `conformance` packages, plus
`errore`.

Not pure, by design: everything else under `adapter/`, and `wiring/` itself.

## 4. Domain model

```go
type Offset uint64        // the address of one entry within a topic
type LogBlock uint64      // a log block, named by the offset of its first entry
type IndexBlock uint64    // the index of the log block with the same number
type TopicName string
```

A **topic** is an ordered sequence of entries addressed by offset, starting at 0. A topic is
stored as a series of **blocks**; a block is named by the offset of its first entry and rolls
over when it passes `MaxBlockSize`. A block is a sequence of **frames**; a frame holds the
entries of one write batch, put through a codec.

```
topic "events"
├── block 0 ──────────────────────────────────────────────┐
│   ├── frame(firstOffset=0,   entries=1000)              │
│   ├── frame(firstOffset=1000, entries=1000)             │  rolls over at MaxBlockSize
│   └── frame(firstOffset=2000, entries=437)              │
└── block 2437 ───────────────────────────────────────────┘
```

**Topic names** are validated by `domain.ValidateTopicName` at every entry point — the topic
aggregate, and the gRPC handlers before that. Rejected: empty, longer than 255 bytes, a
leading dot (which covers `.` and `..`), `/`, `\`, and control characters. A topic name
becomes a directory name in the filesystem adapter, so this is the difference between a name
and an escape.

## 5. Storage formats

Three formats, all little endian, all checksummed with crc32c (Castagnoli).

### 5.1 Entry

The innermost unit, written by `domain.AppendEntry` straight into the frame payload it will be
stored in, and parsed by `domain.ParseEntry` as a window onto the decoded frame, so neither
direction allocates per entry:

```
┌────────────┬──────────────────┬───────────────┬───────────────────┐
│ crc32c  4B │ size  uint64  8B │ entry   size  │ offset uint64  8B │
└────────────┴──────────────────┴───────────────┴───────────────────┘
      │              └──────────── CRC covers size, entry and offset ┘
      └── the check
```

Overhead is 20 bytes per entry. `ParseEntry` is the *only* entry decoder in the codebase, and
since framing only reads use it: recovery, indexing and offset scans work on frame headers and
decode no entries at all. It distinguishes three outcomes precisely:

| outcome | meaning |
|---|---|
| `io.EOF` | the reader ended exactly on an entry boundary — a clean end |
| `io.ErrUnexpectedEOF` | a partial entry — a torn tail |
| `domain.ErrCorruptEntry` | the CRC failed, or the size is larger than `MaxEntrySize` or than what remains |

A size field is never acted on before it has been checked against a bound. That rule is why a
corrupt length is reported rather than allocated.

### 5.2 Frame

A log block is a sequence of frames. Header, 36 bytes:

```
┌───────────┬───────────────┬─────────┬───────────┬────────────┐
│ magic  4B │ headerCrc  4B │ codec 1 │ version 1 │ reserved 2 │
├───────────┴───────────────┼─────────┴───────────┴────────────┤
│ firstOffset  uint64   8B  │ entryCount  uint32           4B  │
├───────────────────────────┼──────────────────────────────────┤
│ storedSize   uint32   4B  │ plainSize   uint32           4B  │
├───────────────────────────┴──────────────────────────────────┤
│ payloadCrc   uint32   4B                                     │
├──────────────────────────────────────────────────────────────┤
│ payload: storedSize bytes — entries, through the codec       │
└──────────────────────────────────────────────────────────────┘
```

`headerCrc` covers the 28 bytes after it, **including `payloadCrc`**. So a header that
verifies can be trusted about how big its payload is and what it must hash to; again, no
length is acted on unchecked.

Three properties earn the header its bytes:

- **A frame can be placed, skipped and checked without being decoded.** That is what lets
  recovery and indexing work on a block written with a codec this binary does not carry.
- **A frame says which codec wrote it**, so a block may hold frames of several codecs and
  changing the codec never rewrites anything.
- **`magic` distinguishes "written before framing" from "corrupt".** A block that does not
  start with `IBSF` is reported as `domain.ErrUnsupportedLogFormat` and never truncated,
  because nothing says those bytes are damaged — with one exception: a block whose first
  header is all zeros is a hole, not an old block, and is cut as torn (§11).

The entry checksum still earns its place inside the frame: the frame checksum catches the
media, the entry checksum catches everything after it, and a frame that verifies whole can
still hold an entry that does not.

### 5.3 Index pair

An index block is a sequence of 20-byte pairs (`index.PairSize`):

```
┌────────────┬─────────────────────┬─────────────────────────┐
│ crc32c  4B │ offset  uint64  8B  │ byteOffset  uint64  8B  │
└────────────┴─────────────────────┴─────────────────────────┘
                 └──── the CRC covers both values ────┘
```

A pair either verifies or is not there — the same rule an entry follows. Parsing stops at the
first torn or failing pair and returns the good prefix; the rest is dropped and rebuilt from
the log. This needs no migration: an index written as bare 16-byte pairs fails at its first
pair and is rebuilt whole, because the index says nothing the log does not.

### 5.4 Where the bytes live

Where these blocks are kept is the adapter's business. `filestore` keeps them at
`<root>/<topic>/%020d.log` and `.idx`; `memstore` keeps them in byte slices; `flashstore`
keeps them in flash pages with a page table in RAM. The core names a block with a
`driven.BlockRef{Topic, Kind, Block}` and nothing more.

## 6. Ports

### 6.1 `driver.LogManager` — the driving port

```go
type LogManager interface {
	List() []domain.TopicName
	Write(topic domain.TopicName, entries domain.EntriesPtr) error
	Read(params ReadParams) error
}
```

The whole of what a log server offers. `grpcapi` consumes it, `core/manager` implements it,
and `embedded.Log` satisfies it — which is why code written against an embedded log works
against a server and back. A driving adapter names this port and never the implementation.

`ReadParams` carries a channel of batches, a `WaitGroup` the caller uses to take them back, a
starting offset, a batch size and a `Cancel` channel. `Cancel` exists because a read can
outlive its caller: a gRPC client that goes away, or a stream whose `Send` fails, must be able
to stop a reader that would otherwise poll forever.

### 6.2 `driven.BlockStore` — the storage port

```go
type BlockStore interface {
	Topics() ([]domain.TopicName, error)
	CreateTopic(topic domain.TopicName) (created bool, err error)
	List(topic domain.TopicName, kind BlockKind) ([]Block, error)
	Append(ref BlockRef, data []byte) (Block, error)
	Open(ref BlockRef, byteOffset int64) (io.ReadCloser, error)
	Truncate(ref BlockRef, size int64) error
	Remove(ref BlockRef) error
}
```

Six block verbs and two topic verbs. It is **deliberately not a filesystem abstraction**:
there are no directories, handles, seeks or permissions, which is exactly why `flashstore`
can satisfy it. `Truncate` is not there for elegance — crash recovery has to cut a torn tail
— and the topic verbs are there because a log server has to enumerate and create topics.

The contract that matters most:

> **`Append` is all or nothing.** On error the block is left as it was. An append that could
> not be rolled back returns an error wrapping `driven.ErrDirtyBlock`, which is how the core
> learns a block must be recovered before it is appended to again.

Implementations must be safe for concurrent use.

### 6.3 `driven.Syncable` — optional durability

```go
type Syncable interface{ Sync(ref BlockRef) error }
```

Probed by type assertion through `driven.Sync`, not required of every store. An in-memory
store has nothing to push, and a microcontroller writing straight to flash has already
persisted the bytes. This is why `memstore` and `flashstore` pay nothing for §9.

### 6.4 `driven.Codec` and `driven.Codecs` — compression

```go
type Codec interface {
	ID() CodecID
	Encode(dst, src []byte) ([]byte, error)
	Decode(dst, src []byte, plainSize int) ([]byte, error)
}
type Codecs map[CodecID]Codec
```

`Codec` chooses what is *written*. `Codecs` is the registry a *read* resolves a frame's codec
byte against, built at wiring time — which is what decides how much compression code a binary
links. `driven.NoCodec` is the identity codec and the only one in the core, so a build with no
compression adapter still writes and reads frames.

> **A codec may never change what it produces for an id.** An id is a promise about bytes
> already on disk.

`Decode` is given `plainSize` from the header and must error rather than return a different
number of bytes.

### 6.5 `driven.Logger` — logging

```go
type Logger interface {
	Log(level Level, msg string, fields ...Field)
	Enabled(level Level) bool
}
```

Fields are typed values (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Err`), so an adapter
switches on `FieldKind` exhaustively and never reaches for reflection. `Enabled` lets the core
skip building a payload that would be discarded. `driven.NopLogger` is the default, which is
what lets an embedded build carry no logging code at all.

### 6.6 `driven.SingleIbsenWriterLock` — coordination

```go
type SingleIbsenWriterLock interface {
	AcquireLock() bool
	ReleaseLock() bool
}
```

Two methods, no token — see §16 for why that is deliberate and what it costs. `NoFileLock` is
the no-op adapter for a single-process or embedded deployment.

## 7. Adapters

| adapter | kind | dependencies beyond stdlib | notes |
|---|---|---|---|
| `blockstore/filestore` | driven | none | the filesystem adapter, and the only one. Its `FS` seam — seven methods and a handle — exists so a fault can be injected below the store, and `*os.File` already satisfies the handle. `DropCache`, the seventh, is `posix_fadvise(DONTNEED)` on Linux (§9.6) |
| `blockstore/memstore` | driven | none | byte slices in RAM; in-memory server mode wires this |
| `blockstore/flashstore` | driven | none | a fixed region of raw flash: fixed pages, write-once bytes, page table in RAM |
| `blockstore/faultfs` | driven (tests) | none | `CrashFiles` tears a write at a chosen byte and fails everything after; `PageCache` models what a power cut keeps — unsynced data, undurable names, Linux fsync failures — on a real directory. See [TESTING.md §5](TESTING.md#5-the-fault-models) |
| `blockstore/conformance` | test support | none | the suite every store passes |
| `compression/zstd` | driven | `klauspost/compress` | `driven.Codec` |
| `locking` | driven | `uuid`, `zerolog` | the file lease |
| `logging/zerologger` | driven | `zerolog` | `driven.Logger` |
| `telemetry` | driven | OpenTelemetry | exporter, lifetime owned by `wiring` |
| `driver/grpcapi` | driving | gRPC | server and Go client |
| `driver/stdio` | driving | none | the log over two byte streams: `Append`, `Cat`, `List`. Stdlib-only, so it is held to rule 1 with the core |
| `driver/cli` | driving | cobra | flags and environment only; it names codecs and directories, it does not build them |
| `driver/history` | driving (tests) | none | records what clients did through `driver.LogManager` and checks it against the log read back; the nemesis and crash-at-every-call harnesses. See [TESTING.md §6](TESTING.md#6-the-history-checker) |

**afero is gone.** It was the storage port before `BlockStore` existed and became a second
filesystem abstraction underneath our own. Removing it cost 1.63 MB of binary — the same
program was 3.40 MB on the afero-backed store and is 1.68 MB on `filestore` — because afero
pulls in `net/http` and `golang.org/x/text` for an HTTP filesystem and unicode normalisation
a log server has no use for. It also cost correctness twice, both times by behaving unlike a
real filesystem: it rejects `O_RDWR|O_EXCL` on an existing file, which broke lease renewal,
and its `OpenFile` checks and creates under separate locks, so `O_CREATE|O_EXCL` is not
atomic there. Every test now runs against a real directory.

## 8. The topic aggregate

`core/topic.Topic` is where the interesting invariants live. Its state — block lists, next
offset, head block size, index position — is guarded by `Topic.mu`.

### 8.1 The write path

```
Write(entries)
  ├─ validate the topic name
  ├─ append()                                      ── under t.mu
  │    ├─ refuse if closed, if a previous write left the block dirty,
  │    │  or if a flush of this topic has ever failed (ErrFlushFailed, §9.3)
  │    ├─ if the head block is past MaxBlockSize: sync it (flusher.barrier, §9.4),
  │    │  then start a new block — the old one is whole before the new one exists
  │    ├─ build frames: AppendEntry per entry straight into the payload, offsets from
  │    │  NextOffset; frames bounded by MaxFrameEntries (1000) and MaxFrameBytes (1 MiB),
  │    │  each built in the buffer the block is appended from (AppendFrame)
  │    ├─ Store.Append(logRef, allFramesAsOneBuffer)     ── one call, all or nothing
  │    ├─ on failure: the store has rolled back; if it could not, ErrDirtyBlock, and
  │    │              writes are refused until LoadOrCreate recovers the block
  │    ├─ advance NextOffset and HeadBlockSize
  │    └─ register the append with the flusher, returning a pending flush
  └─ flush.wait(pending)                           ── outside t.mu: a sync can be slow
       └─ returns once the entries are on durable media; only then are they readable
```

Three details are load bearing. **One `Write` reaches the store as exactly one `Append`**,
however many frames it becomes, so the store never holds half a write and a flush never lands
inside a frame. **Waiting happens outside the topic lock**, so a slow fsync does not block
readers, which take that lock — with one exception, the rollover barrier, which syncs under it
once per block. And **the manager adds no lock of its own**: writers to one topic are inside
the flush policy together, which is what lets them share a sync (§9.2).

Indexing is started by the write that dirtied the index (§12) and never blocks it.

### 8.2 The read path

```
Read(params)
  └─ snapshot()                                    ── under t.mu.RLock, copies block lists
       └─ read()
            ├─ end boundary = the durable offset, not NextOffset, taken once (§9)
            ├─ find the block containing From
            ├─ index.FindNearestByteOffset(From)   ── sort.Search, the pair at or before From
            ├─ Store.Open(logRef, byteOffset)
            ├─ skip whole frames by their stored size until the frame holding From
            ├─ decode that frame; drop the entries in front of From
            ├─ send batches of BatchSize to LogChan, watching Cancel
            └─ the blocks after it, up to the same boundary; stop at the first block
               that starts at or past it
```

A read works on a `snapshot()` — a copy of the topic's block lists and parameters, sharing the
store and the flusher — so **slow consumers never block writers**. Because an index pair
points at a frame boundary and a frame is decoded whole, a read scans at most one frame plus
the sparsity.

**A read runs to the boundary it began with, for every block.** It used to ask the flusher
again for each block after the first, so a flush landing mid-read moved the boundary: the
first block had been cut at the old one, the entries that became durable at its end were
never sent, and the read carried on from the next block's first offset. A tailing reader lost
them for good. A reader that wants more reads again, from where it stopped.

### 8.3 Failure states

| state | cause | escape |
|---|---|---|
| `writeFailure` set | an `Append` failed and the rollback truncate also failed | `LoadOrCreate`, which recovers the head block |
| `ErrFlushFailed` | a sync of this topic failed, in a flush or a rollover barrier | none in this process: the topic is opened again by the next one (§9.3) |
| `ErrTopicClosed` | `Close` was called | none; loaded topics stay readable |
| `ErrDirtyBlock` from the store | an append could not be rolled back | recovery on next load |

## 9. Durability

**A write is acknowledged, and its offsets become readable, only once the flush covering them
has returned.** A reader never sees an entry a power cut could take back.

`NextOffset` is still the next offset to assign. The read boundary is the **durable offset**,
which trails it by whatever is appended but not yet flushed. `endBoundaryForReadOffset`
returns it, and `snapshot()` carries the flusher so a read is bounded by it.

### 9.1 Policy

| parameter | zero means | effect |
|---|---|---|
| `FlushEntries` | `DefaultFlushEntries` = 1 | how many entries may wait before a flush is forced |
| `FlushInterval` | never hold back | how long a batch may wait hoping for more entries |

### 9.2 No background goroutine

The writer that needs its entries durable drives the flush; writers whose entries joined the
same batch wait on it. The core starts no timers it does not own and no goroutine outlives a
topic.

Once driving, a driver takes each batch **as it finds it** rather than re-applying the
policy: the next batch formed while the previous one was syncing, so it has already waited.

Writers arriving while a sync runs append behind it and join the next batch, so even the
default policy coalesces under concurrent load: eight gRPC clients measured 0.33 fsyncs per
write at 0.78 ms, against one fsync per write at 2.03 ms while the manager still held a
per-topic lock across the whole of `Topic.Write`.

Every sync the flusher makes holds `flusher.syncing` across the sync **and** the recording of
its outcome, so syncs and failures have one order and a sync that returns after another has
failed is never taken as proof of anything. A batch's blocks are synced oldest first, which
also keeps the calls a write makes the same from run to run.

```
writer A ──append──┐
writer B ──append──┼──▶ batch 1 ──▶ A drives Sync ──▶ all three return
writer C ──append──┘                     │
writer D ──append──────────────▶ batch 2 ┘ formed while batch 1 synced;
                                           D drives it immediately
```

### 9.3 Failure

**A failed flush stops the topic.** It is returned to every writer waiting on it and to every
batch that formed behind it, the durable offset stays where it was, and every later write is
refused with `ErrFlushFailed` before it reaches the store, for the life of the `Topic`.

A failed fsync is not retryable. Linux (since 4.13) marks the pages it could not write clean
and drops them from writeback: they stay readable from the cache, a later fsync succeeds
without writing them, and after a power cut the range reads as zeros. Retrying — which is what
this code used to do — acknowledges the next write behind a hole that recovery then truncates
at, taking the acknowledged write with it. PostgreSQL stops on a failed fsync for the same
reason.

A failed directory sync is a failed flush too (§9.5): the bytes are on the media, but a block
whose name is not is a block a power cut takes.

### 9.4 A rollover waits for the old block

Before a write rolls over to a new block, `append` syncs the old head through
`flusher.barrier`. Without it, the old block's tail could still be unsynced when the new block
was created, and writeback may put the new block on the media first: after a power cut the
block before the head was then short — a permanent hole in the offsets — or torn, and nobody
could read past it, because recovery only examines the head. With it, **a block before the
head is always whole**, and recovery is right to look only at the head.

It costs one fsync per block, one per gigabyte at the default `--maxBlockSize`, taken under
`Topic.mu`, so readers wait out that one fsync to take their snapshot. A failed barrier is a
failed flush.

### 9.5 Names are durable too

An fsync of a file says nothing about the names it is reached by. `filestore.Sync` makes three
things durable: the block, its name in the topic directory, and the topic's name in the root.
The two directory syncs happen once per block and once per topic **in the life of a store**,
on the first sync of each, and never again — appending to a block already named on the media
adds no name, and the fsync an acknowledged write pays is the one a log can least afford to pay
twice. Once per store rather than once per block created, because a store cannot know that a
name it found was ever made durable: the process before it may have created it and failed to
sync its directory, with the name still in the cache.

### 9.6 A restart reads the media, not the cache

The pages a failed fsync dropped are clean, so they outlive the process that saw the failure,
intact and readable in the page cache. A process started after it could recover the head block
from there, find it whole, append behind the hole and acknowledge writes the next power cut
took. So the first time a store lists a topic, it drops the head block's clean pages
(`filestore.FS.DropCache`: `posix_fadvise(DONTNEED)` via `syscall` on linux/amd64, arm64 and
arm, a no-op elsewhere), and recovery reads what the media holds.

Only the head, because a failed fsync stops the topic before it can roll over. Only for a
writable store: `filestore.ReadOnly` keeps the cache, since a reader recovers nothing and every
`ibsen cat` would otherwise read the log cold. It is advice, and the kernel keeps pages that are
dirty, mapped or under writeback — a dirty page is one it will still write, so that is not a
hole. Off Linux, or where the kernel declines, recovery sees what the cache holds.

### 9.7 Index blocks are deliberately not flushed

The index is derivable from the log, and recovery already drops torn pairs and re-indexes, so
paying an fsync for it would buy nothing.

## 10. The index

One sparse index block per log block: a pair every `IndexSparsity` entries (default 10),
mapping an offset to a byte offset within the block.

- **Lookup is `sort.Search`** for the first pair past the target, returning the one before it.
  The pairs are appended in scan order, so they are already sorted. A zero pair means "nothing
  at or before this — scan from the start of the block", which is reachable for a block that
  does not begin on a multiple of the sparsity. `core/index/find_test.go` keeps the linear
  scan this replaced and asserts the two agree for every query across seven index shapes.
- **Sparsity is per-topic state**, copied by `snapshot()`, and changing it between runs is
  safe: pairs already written stay valid and sorted, and the block ends up indexed at two
  densities. `CreateBinaryIndexFromLog` returns `ErrInvalidSparsity` for 0 rather than
  reaching `offset % 0`, which panics.
- **Pairs are checksummed** (§5.3). Without this a corrupt pair pointed at a byte that is not
  an entry boundary and the read from it failed on EOF.
- **`IndexPosition.ByteOffset` always means "the end of the scanned region."** Indexing
  resumes from there.
- **Since framing, a pair points at the start of a frame, never inside one.** A frame is
  decoded whole, so there is nothing finer to aim at. A frame earns a pair when it covers an
  offset that is a multiple of the sparsity — the same set of pairs the entry-wise rule gave
  when every entry had its own frame, and one pair per frame once frames are larger than the
  sparsity.

## 11. Recovery

`logfmt.RecoverBlock` runs when a topic loads, on its head block — after the store has dropped
that block's clean pages from the cache (§9.6), so it reads what the media holds:

```
RecoverBlock(store, ref, firstOffset, blockSize)
  ├─ the first header is all zeros                       ─▶ torn at byte 0
  ├─ scan frames from the start of the block
  │    ├─ header does not verify, or payload CRC fails  ─▶ valid region ends here
  │    ├─ payload is shorter than storedSize            ─▶ valid region ends here
  │    └─ a valid frame with an unexpected firstOffset  ─▶ error, truncate nothing
  ├─ truncate the block to the end of the valid region
  └─ return (nextOffset, validSize, truncatedBytes)
```

Rules worth stating explicitly:

- **A block that does not start with the frame magic is `ErrUnsupportedLogFormat`**, and
  nothing is truncated. Written before framing, or by a newer Ibsen — either way nothing says
  the bytes are damaged. This is a deliberate clean break: the log is not derivable the way
  the index is, so the magic exists purely to tell the two cases apart.
- **A block that starts with zeros is torn, not pre-framing.** Its first write was in the page
  cache and never on the media, because its fsync failed, and the size reached the media
  anyway. A block written before framing starts with the checksum of its first entry, which
  is zero once in four billion.
- **Only the head block is recovered**, and that is enough: a rollover syncs the old head
  before the new block exists (§9.4), so every block before the head is whole. Blocks damaged
  by builds older than that are not looked for.
- **A frame whose codec this build does not carry is still recoverable**, because scanning
  needs only the checksums. The missing codec is reported when a read asks for the entries.
- **Complete entries of a batch whose write returned an error can survive recovery**, as with
  any crash. There are no batch markers.
- **The index is cut to match.** Pairs past the recovered end, and torn partial pairs, are
  dropped from the head index, and indexing resumes right after the last kept entry.

## 12. Indexing without timers

`Topic.UpdateIndex` coalesces instead of dropping work:

```
UpdateIndex()
  ├─ indexPending = 1                          ── the mark goes up first
  ├─ CAS(indexing, 0→1) fails ─▶ return false  ── somebody else will take this work
  └─ loop:
       ├─ while SwapInt32(&indexPending, 0) == 1:  index the tail
       ├─ indexing = 0
       └─ if indexPending came back up, try to take the run again
```

The mark goes up before the exclusion flag is tested, so a run on its way out sees it before
it decides to stop. **The tail of a log is always indexed by somebody.**

This replaced a ten-second ticker in `manager.NewLogTopicsManager` that swept every loaded
topic to catch work its exclusion flag had dropped. The topic now takes that work itself, so
the core starts no goroutine that outlives the call which made it, and an embedded build
carries no timer it did not ask for.

## 13. Compression

### 13.1 How it works

- Every frame carries one byte naming its codec. A block may hold frames of several codecs.
- **Compression chooses only what is written.** The read registry holds every codec the binary
  links, so turning compression off, or changing it, never strands a block written under the
  old setting.
- **The level is not part of the format.** A frame says only that zstd wrote it.
- **Compression that did not pay is discarded.** `AppendFrame` falls back to `CodecNone` and
  the plain bytes when the encoded payload is not smaller, so turning compression on can never
  make a topic larger.
- An unknown codec byte is `driven.ErrUnknownCodec`: the deployment is missing a codec, not
  the data. Nothing is truncated.

### 13.2 The frame bound, measured

`adapter/driven/compression/zstd/bench_test.go` varies `MaxFrameEntries` with `MaxFrameBytes`
wide open, on one write of 10000 ~130-byte JSON events through zstd at the default level,
over `memstore` so the flush policy stays out of the numbers:

| entries/frame | ratio | decoded per single-entry read | single read | sequential read |
|---|---|---|---|---|
| 1 | 1.36 | 410 B | 69 µs | 122 MB/s |
| 10 | 0.30 | 1.6 KB | 96 µs | 121 MB/s |
| 100 | 0.17 | 14 KB | 64 µs | 215 MB/s |
| **1000 (default)** | **0.16** | **137 KB** | **248 µs** | **403 MB/s** |
| 10000 | 0.17 | 1.4 MB | 2537 µs | 428 MB/s |

Reading it: the compression ratio is flat above 100 — 1000 buys 4% over 100 and 10000 buys
nothing. Read amplification is linear in the bound, because a frame is decoded whole.
Sequential throughput wants the opposite and flattens around 1000.

**The bound is therefore a choice between random and sequential readers, and the default of
1000 chooses sequential** (decided 2026-09-19): reading forward through a large log is the
workload Ibsen is for, and 100 would give up nearly half the sequential throughput. A
deployment whose readers seek rather than stream should set `--maxFrameEntries 100`. Nothing
already written is affected either way, because a frame carries its own size.

A frame per entry is where a codec has nothing to offer: zstd on 130 bytes comes back larger,
so every such frame falls back to plain bytes, and the 1.26× there is framing overhead alone.
The index also gets only one pair every *sparsity* frames, so small frames cost header
scanning too.

Not measured yet: the CPU of a compression attempt that is then discarded. Skipping it below
a size threshold would save that, and wants its own benchmark.

## 14. Composition roots

Two packages, not two build tags. Nothing is excluded; a program imports the root it wants.

### 14.1 `wiring` — the server

`wiring.IbsenServer` builds every adapter and owns the lifecycle:

```
Start(listener)
  ├─ defaults()            build the adapters not injected: file lease at <root>/.writeLock,
  │                        or NoFileLock for in-memory mode
  ├─ resolveCodecs()       "zstd" → build the adapter; unknown name → ErrUnknownCompression
  ├─ blockStore()          memstore | filestore.NewOS | filestore.ReadOnly
  ├─ AcquireLock()         refused → ErrWriteLockUnavailable (matchable with errors.Is)
  ├─ telemetry exporter    lifetime held here, not by the gRPC adapter
  ├─ manager.NewLogTopicsManager
  ├─ startGRPCServer
  └─ wait for shutdown     Start returns only once the shutdown has finished,
                           because the process exits when it returns
```

`Start` returns its failures instead of exiting, so a program embedding the log decides what
to do; the CLI reports and exits.

**Read-only mode builds a read-only store, not just a manager that refuses writes** —
`filestore.ReadOnly` refuses every call that would change a file. Loading a topic recovers its
head block and rebuilds its index, so a read-only server without a read-only store would
quietly rewrite a directory another instance owns.

**In-memory mode wires `memstore`**, reaches no filesystem at all and takes no write lock: the
log lives in the process and is shared with nobody. `wiring/ibsen_test.go` pins this by
writing through a running in-memory server and then walking the filesystem it was given to
check nothing landed on it.

### 14.2 `wiring/embedded` — the library

```go
log, err := embedded.Open(embedded.Params{Store: memstore.New()})
```

`Open` returns a `*Log` that satisfies `driver.LogManager`. The store is **injected, not
chosen**: this package imports nothing but `core/`, so a program bringing `memstore`,
`flashstore` or `filestore` stays inside the standard library, and one wiring the zstd codec
pays for zstd. The logger and codec default to `NopLogger` and `NoCodec`.

`ErrNoStore` is returned when no store is given. There is no default, because where the bytes
go is the one decision an embedded build has to make, and guessing would be the package
choosing a dependency for the program that imports it.

Measured by `scripts/embedded-size.sh` on go1.26.4, `CGO_ENABLED=0 -ldflags="-s -w"`:

| target | server | embedded | |
|---|---|---|---|
| linux/amd64 | 16.48 MB | 1.87 MB | 8.8× smaller |
| linux/arm | 15.44 MB | 1.81 MB | 8.5× smaller |

The dependency graph is the gate, since it is exact; the size is the other side of the same
claim — what the linker produced rather than what the imports promised. The script fails if an
embedded build stops being smaller, rather than enforcing a byte ceiling that would drift with
each Go release.

### 14.3 `wiring.OpenLocal` — a data directory, no server

```go
local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: dir})
```

The root behind `ibsen append`, `ibsen cat` and `ibsen topics`. It builds `filestore`, the
codecs and the single-writer lease, and returns a `*LocalLog` embedding `*embedded.Log`, so it
satisfies `driver.LogManager` like everything else.

`ReadOnly` decides two things at once: the store becomes `filestore.ReadOnly` and no lease is
taken. That is what makes a reader safe to point at a directory another instance owns — a load
recovers the head block and could otherwise truncate a torn tail underneath the writer. A
writable open takes the lease at `<root>/.writeLock` and is refused with
`ErrWriteLockUnavailable` while a server holds it.

`buildCodecs` is shared with the server's root, so a compression name resolves identically in
both and a topic written by one is read by the other.

## 15. Concurrency and shutdown

| guarantee | how |
|---|---|
| Readers never block writers | `Read` works on `snapshot()`, taken under `RLock` |
| A slow fsync never blocks readers | the flush wait happens outside `Topic.mu`; the one exception is the rollover barrier, once per block |
| Concurrent writers to a topic share a flush | the manager takes no lock of its own; `Topic.append` assigns offsets under `Topic.mu` and the flusher batches what arrives during a sync |
| Syncs and sync failures have one order | `flusher.syncing` is held across each sync and the recording of its outcome |
| At most one first load per topic | `getOrCreateTopic` runs one load; concurrent requests wait for it and share its result. A failed load is not cached, so the next request retries |
| `Start` and `shutdown` may race | `IbsenServer.mu` guards `topicsManager`, `grpcServer` and the lifecycle channels; `lifecycle()` makes the channel pair once; `grpcapi` guards its `*grpc.Server` and records a `Stop` that arrives before the server exists |
| `Close` never races an index `Add` | only the `Write` goroutine adds to `Topic.indexWg`, under `Topic.mu` before `closed` is set |

Before the load guard existed, two first requests could both run `LoadOrCreate` and keep one;
the discarded load kept recovering the head block and rewriting its index while the kept topic
accepted writes, which could truncate acknowledged entries.

Shutdown order is the part that has to be right:

```
ShutdownCleanly()
  ├─ gRPC stops           (a forced stop does not wait for handlers)
  ├─ manager.Close()      refuse new writes and loads (ErrClosed), wait for those in flight,
  │                       close each topic — Topic.Close refuses writes and waits for the
  │                       indexing earlier writes started; loaded topics stay readable
  └─ release the write lock       ── last, so nothing writes after the lock is gone
```

Calls arriving during shutdown get `Unavailable`.

The exception is losing the lease (§16): that path **exits** rather than shutting down
cleanly, because a clean shutdown flushes and writes.

## 16. Single writer, and distribution

### 16.1 What the file lease does

`adapter/driven/locking.FileLock` keeps a lease in `<root>/.writeLock`. It asks a real
filesystem for two things and gets both:

- **`O_CREATE|O_EXCL` is atomic**, which makes a claim on a free lock a claim rather than an
  open. Before this it used `O_CREATE` alone, and two servers starting together both created
  the file and both came away holding the lease.
- **`Rename` replaces a file atomically.** Every claim is written to a temporary file and
  renamed into place, so a reader sees the old holder or the new one, never a half-written
  name. The truncate-then-write this replaced produced real short reads under contention,
  which a holder renewing its own lease read as having lost it.

An expired lease has no such primitive — a filesystem has no compare-and-swap — so a takeover
is **confirmed rather than assumed**: the claimant pauses for a tenth of the lease, reads the
file back, and only the instance whose id is in it carries on.

**Renewal proves the lease is still its own before extending it.** This closes a window that
needs no network partition to open: the holder stalls past its lease (a GC pause, a frozen
scheduler, a slow disk), the lease expires, a second instance legitimately claims it, the
first wakes up and — under the old code, which truncated and wrote its own id without reading
first — steals it back. Both then believe they hold it. Renewal also treats its own lease
having aged out as lost even if nobody has taken it, since anyone may take it at any moment,
and `ReleaseLock` will not remove a lock file it no longer holds, which would hand the log to
a third writer.

Losing the lease is reported through `locking.LeaseLost` rather than exited from inside the
adapter. `wiring.IbsenServer.writeLockLost` stops the process, and it **exits** rather than
shutting down cleanly.

This is a read followed by a write, not an atomic renewal. It closes the window a stall opens;
it does not remove it.

### 16.2 Why there is no Raft

Ibsen does not implement replication or consensus. Run it on replicated storage — a Kubernetes
StatefulSet on a replicated volume — and keep single-writer safety with a fencing lease and a
monotonic token from a system that already holds elections (etcd lease and mod-revision, for
instance).

That deployment already fences below Ibsen: a StatefulSet gives at most one pod per ordinal,
and a ReadWriteOnce volume is attached to one node by the CSI driver. That has known edges
around force-detach and unreachable nodes, which is a reason to keep the file lease as a cheap
backstop rather than to build etcd integration.

### 16.3 What a fencing token would actually take

- **A token cannot reach storage without widening a port that is deliberately narrow.**
  `AcquireLock() bool` has no token and `Append(ref, data)` has no token, and threading one
  through would make `memstore`, `flashstore` and every microcontroller store carry a concept
  one deployment needs.
- **It does not have to.** A fencing store is a `BlockStore` **decorator** built in `wiring`,
  checking the token on every append rather than once at startup. No core change, no port
  change.
- ⚠️ **Any `BlockStore` decorator must forward `Syncable`.** It is probed by type assertion in
  `driven.Sync` and in `newFlusher`, so a wrapper that does not implement it makes both
  conclude the store has nothing to flush. Nothing fails; the durability guarantee of §9 just
  quietly stops holding.
- **Storage-side enforcement is not reachable through a filesystem**, which has no conditional
  write. Getting it means an object store with `If-Match`, and object stores do not append,
  while `BlockStore` is built around appending to a growing block. That is a storage redesign,
  not a feature.

## 17. Testing

Testing is the linchpin: the architecture is only worth the claims it lets you check. The
strategy, the fault models, the two system-level harnesses and how to work with them are in
**[TESTING.md](TESTING.md)**; this is the shape of it.

| level | what it checks | where |
|---|---|---|
| static | the core is pure, dependencies point inward, an embedded build is small, no zerolog event goes unsent | `scripts/`, `logcalls_test.go` |
| formats | every byte of every header is covered by its checksum; recovery against every kind of damage | `core/domain`, `core/index`, `core/logfmt` |
| port conformance | one suite, every `BlockStore` — including `filestore` over the power-cut model | `blockstore/conformance` |
| core properties | read from every offset, on every store, live and reloaded, with and without an index | `core/topic/topicAccess_property_test.go` |
| contracts | durability, fail-stop, coalescing, one load per topic, shutdown — with gates deciding the interleaving | `core/topic`, `core/manager` |
| faults | torn writes (`CrashFiles`); power cuts, undurable names and Linux fsync failures (`PageCache`) | `filestore/*_test.go`, `core/topic/*_test.go` |
| adapters and roots | each adapter's own job; each composition root's wiring | `adapter/...`, `wiring/...` |
| end to end | a real gRPC server on a free port | `adapter/driver/grpcapi/test` |
| system | a history checker over concurrent clients; a randomised nemesis; a crash at every storage call, with every promise checked for durable support | `adapter/driver/history` |

**Everything runs against real directories.** Nothing emulates a filesystem: an emulation that
disagrees with a real filesystem is worse than not running at all, and the one this project
used disagreed twice, both times about guarantees the lease depends on. The power-cut model is
not an emulation in that sense — every call happens on a real directory, and only what survives
a power cut is modelled.

## 18. Extending Ibsen

### A new storage backend

Implement `driven.BlockStore`; add `Syncable` if your medium has a volatile buffer. Then run
the conformance suite against it — that is the whole bar, and it is the only reason to trust
`flashstore` — and add it to `coreBackends()` so the core's property tests run on it. Keep it
stdlib-only if you want embedded builds to stay small. [TESTING.md §10](TESTING.md#10-recipes)
has the full recipe.

### A new codec

Implement `driven.Codec` with an id nobody else uses, and remember the promise: **an id may
never change what it produces**, because it describes bytes already on disk. Register it in
the composition root, not in a driving adapter.

### A new transport

Implement against `driver.LogManager` and nothing else. Do not import a driven adapter —
rule 3 of the architecture check will catch you, which is the point. `adapter/driver/stdio` is
the smallest example: it drives the log over two byte streams, imports only `core/`, and is
held to that by rule 1 along with the core itself.

### A decorator

Wrap `BlockStore` in `wiring`. Forward `Syncable` (§16.3). Remember that `Append` must stay
all-or-nothing, and that a failure you cannot roll back must wrap `driven.ErrDirtyBlock`.

## 19. Invariants

The rules that must not be broken, collected:

1. `core/` imports only the standard library, and only the subset TinyGo supports.
2. `core/` never imports `adapter/` or `wiring/`; a driving adapter never imports a driven one.
3. No length read from storage is acted on before it has been checked.
4. `Append` is all or nothing; a failure that cannot be rolled back wraps `ErrDirtyBlock`.
5. One `Write` reaches the store as exactly one `Append`.
6. An entry is readable only once the flush covering it has returned.
7. A codec id is a promise about bytes already on disk.
8. A `BlockStore` decorator forwards `Syncable`.
9. A block that is not damaged is never truncated.
10. The index says nothing the log does not, and may always be rebuilt from it.
11. The core starts no goroutine that outlives the call which made it, and no timer at all.
12. A failed fsync stops the topic; it is never retried.
13. A block before the head is whole: a rollover syncs the old head before the new block exists.
14. A write is acknowledged only once its block's name, and its topic's, are durable too.
15. A read runs to one durable boundary, taken when it begins.
16. A writable store reads a head block from the media, not the cache, before recovering it.

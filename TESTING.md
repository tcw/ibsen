# Testing

This document describes how Ibsen is tested and why it is tested that way. It is written for
developers changing the code: what each level of the suite is for, what the fault models do
and do not model, how to read a failure from the two system-level harnesses, and what a change
to a given part of the log has to come with.

[ARCHITECTURE.md](ARCHITECTURE.md) describes what is being tested. [README.md](README.md) is
the introduction.

- [1. What the tests are for](#1-what-the-tests-are-for)
- [2. Principles](#2-principles)
- [3. The levels](#3-the-levels)
- [4. Level by level](#4-level-by-level)
- [5. The fault models](#5-the-fault-models)
- [6. The history checker](#6-the-history-checker)
- [7. The nemesis test](#7-the-nemesis-test)
- [8. A crash at every storage call](#8-a-crash-at-every-storage-call)
- [9. Running the suite](#9-running-the-suite)
- [10. Recipes](#10-recipes)
- [11. Promises and the tests that hold them](#11-promises-and-the-tests-that-hold-them)
- [12. What the harnesses have found](#12-what-the-harnesses-have-found)
- [13. Known gaps](#13-known-gaps)
- [Appendix: catalogue of test files](#appendix-catalogue-of-test-files)

---

## 1. What the tests are for

A log makes a small number of promises, and almost every test in this repository exists to
check one of them under some condition that makes it hard to keep:

1. **An acknowledged write is durable.** `Write` returns only once the flush covering its
   entries has returned, and after any crash from that moment on the entries are there.
2. **Nothing is readable before it is durable.** A reader is never handed an entry a power cut
   could take back.
3. **A write is whole or a prefix.** One `Write` reaches the store as exactly one `Append`; a
   write that failed may leave a prefix of its entries, never a gap inside them and never more.
4. **Offsets are contiguous and stable.** Offset *n* is the *n*-th entry of the topic, before a
   crash and after it, and a read from an offset returns that offset and the ones after it,
   without skipping.
5. **Recovery never destroys what is not damaged.** A torn tail is cut; a block the build does
   not understand is reported, not truncated.
6. **One writer.** Two processes never append to the same directory.
7. **The core is pure.** It reaches nothing outside the standard library, and nothing outside
   `core/`.

The first four are about the log under failure, and they are where the subtle bugs have been:
every one of the seven correctness bugs found since the fault model existed (§12) broke one of
them, and none of them showed up in a test that did not fail the storage underneath.

## 2. Principles

These are the habits the suite is built on. A change that breaks one of them needs a reason.

### Real directories, and no emulated filesystem

Every test that touches a filesystem touches a real one, under `t.TempDir()`. The emulated
filesystem the project used to run on disagreed with real ones twice — `O_RDWR|O_EXCL` on an
existing file, and `O_CREATE|O_EXCL` not being atomic — and both times about guarantees the
write lease depends on. An emulation that disagrees with a real filesystem is worse than not
running at all, because it passes.

The one place a filesystem is *modelled* is durability, which a real filesystem gives a test
no way to observe: `faultfs.PageCache` (§5) performs every call on a real directory and keeps,
beside it, a model of what the media holds. Opens, creates, reads and directory listings
behave exactly as the kernel makes them behave; only "what survives a power cut" is the
model's answer.

Two habits follow from real directories:

- **A test that writes a file must create its parent directory**, which the old emulation did
  implicitly.
- **A test that builds a manager or a topic on a `t.TempDir()` must close it.** A write starts
  background indexing, and an indexer still writing races Go's removal of the directory. It
  surfaces as `TempDir RemoveAll cleanup: directory not empty`, attributed to whichever test
  was unlucky. `newTestManagerWithStore`, `newTestTopic` and `stoppedByTest` register that
  cleanup.

### A fix comes with a test that fails without it

Every bug fixed in this repository has a test that reproduces it, and the reproduction has
been run against the code *before* the fix to see it fail for the reason it claims. The usual
way is to stash the production change and run the one test:

```shell
git stash push -q core/topic/topicAccess.go
go test ./core/topic/ -run TestReadDoesNotSkipTheEndOfABlockWhenAFlushLandsDuringIt -count=1
git stash pop -q
```

A test that passes either way proves nothing. This has caught tests that were weaker than they
looked — a read test whose assertion a read returning nothing would also have satisfied, and a
crash harness that passed with the fix it was meant to guard removed (§8).

### Check the checker

The two system-level harnesses are only worth the bugs they can find, so they are checked the
same way: revert a fix they are supposed to guard, and see them fail. §8 records which
reverts each harness catches and which loss catches it. When a harness passes a revert it
should have caught, the harness is the thing that is wrong.

### Gates, not sleeps

A concurrency test decides the interleaving it is about. The stores used for that hold a call
until the test lets it go:

| helper | where | holds |
|---|---|---|
| `syncGate` | `core/topic/flush_test.go` | every `Sync`, with an injectable error |
| `heldSyncs` | `core/topic/powerCut_test.go` | every `Sync`, forwarding to the store underneath |
| `secondSyncFails` | `core/topic/flush_test.go` | the first armed sync until released; fails the second |
| `heldSyncStore` | `core/manager/concurrentWrite_test.go` | every `Sync`, counting appends and syncs |
| `gatedLoadStore` | `core/manager/topicLoad_test.go` | the first topic load until a second arrives |
| `gatedWriteFs` | `core/manager/close_test.go` | a file write, below the store |
| `gatedStore` | `adapter/driver/history/crashpoints_test.go` | every sync after the next log append |

Waiting for a state is `eventually(t, what, cond)` with a deadline, never a bare sleep. Where a
fixed wait remains, it may only change how much a test covers, never whether it passes: the
5 ms a crash-harness pair gives its second writer is one such wait, and §8 explains why the
outcome does not depend on it.

Two traps to know about, both of which have bitten this suite:

- **Compute the expected state before starting the goroutine that reaches it.** `want :=
  nextOffsetOf(topic) + 1` written *after* `go writeOne(...)` races the append; if the append
  wins, `want` is one too high and the test waits out its deadline.
- **Do not take a lock the code under test may be holding.** A rollover holds `Topic.mu` while
  it syncs the old block, so a test polling `nextOffsetOf(topic)` during a held sync deadlocks
  itself. Observe the gate instead.

### Test at the port

The core is tested through its ports, against every adapter. `coreBackends()` at the top of
`core/topic/topicAccess_property_test.go` is the only code in the core's tests that knows which
store is behind `driven.BlockStore`; everything else speaks in topics and offsets. A test that
needs a fault below the store composes it there (`filestore.New(faultFS, dir)`), and the core
does not know.

### Determinism where enumeration needs it

A harness that names a crash point by its number in a trace needs the same workload to make
the same calls in the same order every time (§8). That is why the flusher syncs a batch's
blocks oldest first rather than in map order, and why the harness counts no calls on index
files, which a goroutine writes on its own schedule. A change that adds nondeterminism to the
log's calls on log blocks or directories breaks the harness loudly, which is the intent.

### Names say what is true

A test is named for the behaviour it pins, as a sentence where that reads better than a
label: `TestAFailedFlushStopsTheTopic`, `TestPowerCut_aRolloverWaitsForTheOldBlock`,
`TestReadOnlyOpenWritesNothing`. Its comment says why the behaviour matters and, for a
regression test, what used to happen and what found it.

## 3. The levels

The suite is layered from the bytes outward. A level holds what the levels below it cannot see
and leans on them for the rest.

```
                                    ┌───────────────────────────────────────────┐
  L8  system, history-checked       │ history checker · nemesis · crash at every │  minutes of
                                    │ storage call                               │  simulated failure
                                    ├───────────────────────────────────────────┤
  L7  end to end                    │ a real gRPC server on a free port; wiring  │
                                    ├───────────────────────────────────────────┤
  L6  adapters                      │ stdio · cli · grpcapi · zstd · locking ·   │
                                    │ zerologger · wiring roots                  │
                                    ├───────────────────────────────────────────┤
  L5  faults below the store        │ torn writes · power cuts · failed fsyncs   │
                                    ├───────────────────────────────────────────┤
  L4  concurrency and contracts     │ gated stores: flush, indexing, load, close │
                                    ├───────────────────────────────────────────┤
  L3  core properties               │ every offset × every store × reload modes  │
                                    ├───────────────────────────────────────────┤
  L2  port conformance              │ one suite, every BlockStore                │
                                    ├───────────────────────────────────────────┤
  L1  formats and units             │ entries · frames · index pairs · recovery  │
                                    ├───────────────────────────────────────────┤
  L0  static                        │ gofmt · vet · architecture · size · AST    │
                                    └───────────────────────────────────────────┘
```

| level | asks | fault model | where |
|---|---|---|---|
| L0 static | is the code shaped as the architecture says | none | `scripts/`, `logcalls_test.go` |
| L1 formats | are the bytes right, and is every damaged byte caught | corrupted and truncated buffers | `core/domain`, `core/index`, `core/logfmt` |
| L2 conformance | does every store keep the port's contract | none | `blockstore/conformance`, run by each store |
| L3 properties | does the log read back what was written, from every offset, on every store | reloads, missing indexes | `core/topic/topicAccess_property_test.go` |
| L4 contracts | do concurrent callers get what the contract says, in the interleavings that matter | gated calls, injected errors | `core/topic`, `core/manager` |
| L5 faults | what survives a crash, a power cut, a failed fsync | `CrashFiles`, `PageCache` | `filestore/*_test.go`, `core/topic/*_test.go` |
| L6 adapters | does each adapter do its job and only its job | per adapter | `adapter/...`, `wiring/...` |
| L7 end to end | does a client reach the log through a real server | none | `adapter/driver/grpcapi/test` |
| L8 system | does one log explain everything every client saw, after failures | `PageCache`, all of it | `adapter/driver/history` |

Benchmarks sit beside the levels rather than on top of them, and CI does not run them (§9).

## 4. Level by level

### L0 — static checks

CI runs these before a single test:

- **`gofmt` and `go vet`.** Both clean, always.
- **`scripts/check-architecture.sh`.** Three rules: the core, the embedded root, the stdio
  adapter and the stdlib-only stores reach nothing outside the standard library (`go list
  -deps`, so transitively); nothing under `core/` imports `adapter/` or `wiring/`; no driving
  adapter imports a driven one, except test-support packages. It prints nothing when the
  architecture holds.
- **`scripts/embedded-size.sh`.** Builds the server and the embedded example and fails if the
  embedded build stops being smaller. The dependency graph is the exact gate; this is the same
  claim from the linker's side.
- **`logcalls_test.go`.** Parses every production file and fails on a zerolog event that is
  built and never sent: `log.Fatal().Err(err)` without `.Msg` neither logs nor exits, and 27
  of them once did exactly that.

`staticcheck -checks U1000 ./...` prints nothing — there is no unused code — but CI does not
run it.

### L1 — formats and units

Byte-level tests of the three on-disk formats and the code that reads them. The rule they hold
is that **no length read from storage is acted on before it has been checked**, so a damaged
byte is reported, not allocated or followed.

- `core/domain/frame_test.go`: the 36-byte header round trip; **every one of its 36 bytes**
  flipped and caught by its checksum; a missing magic; a future version; a stored size larger
  than the block; read, verify and skip of a payload agreeing across the streaming buffer
  boundary.
- `core/domain/fsUtils_test.go`: the entry codec round trip; `ParseEntry` distinguishing a
  clean end, a partial entry and a corrupt one; and the allocation pin — encoding and parsing
  allocate nothing per entry, and a parsed entry aliases the buffer it came from. The pin is
  skipped under `-race`, where the detector allocates on its own account; `race_test.go` and
  `norace_test.go` are a build-tag pair that tells it which build it is in.
- `core/index`: checksummed pairs (every byte covered, parsing stopping at a corrupt or torn
  pair, the old format rejected), `sort.Search` lookup checked against the linear scan it
  replaced across seven index shapes, and sparsity.
- `core/logfmt`: `RecoverBlock` against a partial header, a partial payload, a garbage tail, a
  corrupt frame, a valid frame at an unexpected offset (an error, nothing truncated), a block
  written before framing (reported, not truncated), a block starting with zeros (torn, cut),
  and a frame whose codec is not wired (recovered anyway); reads rejecting corrupt frames and
  corrupt entries inside valid frames; `AppendFrame` building into the block buffer and leaving
  nothing behind when it fails; compression falling back to plain bytes when it does not pay.
- `core/port/driven/codec_test.go`: the codec registry — identity always present, an unwired
  codec named in the error, a nil registry still reading plain frames.
- `errore`: the error helper used on both sides of the hexagon.

### L2 — port conformance

`adapter/driven/blockstore/conformance.Run(t, newStore)` is one suite every `BlockStore` passes:
round trip, read from every byte offset, append creating topic and block, an empty append,
topic creation and listing, kinds kept apart and blocks ordered, unknown topics and blocks,
truncate bounds and truncate-then-append, remove dropping only its block, a reader that keeps
reading across appends, `Sync` being optional, and concurrent appends and reads.

It runs against `filestore` on a real directory, `memstore`, `flashstore` on a RAM device, and
`filestore` over `faultfs.PageCache` — the last one being what says the fault model changes
nothing a store can observe until it crashes. **Passing it is the whole bar for a new store**,
and it is the only reason to trust `flashstore`, whose pages and write-once bytes are as far
from a file as the port goes.

### L3 — core properties

`core/topic/topicAccess_property_test.go`, against every store in `coreBackends()`:

- **Read from every offset.** For block sizes of 64 B, 500 B, 2000 B and 1 MiB, and in four
  modes — live, reloaded, reloaded with every index block removed, reloaded with only the
  newest removed — read from every offset at batch sizes 1, 7 and 1000, and check every entry
  is the one written there.
- **Concurrent write, read and index.** Writers, readers and indexing at once, every read
  contiguous and correct.

These are slow on purpose (they are most of `core/topic`'s minute under `-race`) and they are
the reason a change to framing, indexing or block rollover can be made with confidence.

### L4 — concurrency and contracts

Tests that decide an interleaving with a gate (§2) and check the contract in it.

- **Durability** (`core/topic/flush_test.go`): a reader sees nothing until the flush returns,
  and `Write` does not return either; a failed flush is reported, leaves nothing readable and
  refuses every later write; a batch that formed behind a failed flush fails with it; a failed
  rollover sync stops the topic, and a flush finishing after it is not acknowledged;
  concurrent writers share one sync; the interval releases a writer that never reaches the
  count; a store that cannot sync never waits; a reloaded topic counts its recovered block as
  durable.
- **Reads across a flush** (`readAcrossFlush_test.go`): a read spanning two blocks while a
  flush lands does not skip the end of the first.
- **Indexing** (`indexing_test.go`): a request during a run is picked up, not dropped; sixteen
  callers coalesce into one run; a failed run leaves the work.
- **The manager**: one load per topic however many first requests arrive, and a failed load
  not cached (`topicLoad_test.go`); `Close` waiting for writes and indexing in flight
  (`close_test.go`); concurrent writers to one topic appending behind a held flush and sharing
  the next one (`concurrentWrite_test.go`); no goroutine started by building a manager, and the
  index complete once writes stop.

### L5 — faults below the store

Two fault filesystems sit under `filestore`'s seam (§5). Tests at this level live beside the
code they test:

- `filestore/crash_test.go` — process crashes with `CrashFiles`: a prefix of an append lands;
  a crash during an append is a dirty block; a crash before any byte leaves the block as it
  was; a crash after the whole append keeps it; a crash during truncate leaves the longer
  block.
- `filestore/powerCut_test.go` — power cuts with `PageCache`: a synced block in a new topic
  survives; a sync cut between the block's fsync and its directory's is not durable; a restart
  retries the directory sync that failed, for the topic directory and for the root.
- `filestore/dirsync_test.go`, `durability_test.go`, `evict_test.go` — what `Sync` costs and
  does, counted through the seam: the directory synced once per block and the root once per
  topic in the life of a store, a failed directory sync reported and retried, synced bytes on
  disk, the head block dropped from the cache once per topic and never by a read-only store.
- `core/topic/topicAccess_crash_test.go` — the core recovering from a torn write: every
  acknowledged entry kept, the torn batch gone, the log writable and readable end to end.
- `core/topic/powerCut_test.go` — the core under power cuts: a failed fsync is not retried; a
  restart after a failed fsync does not write behind the hole, with the hole in and at the
  start of the block; a rollover waits for the old block, with its tail missing, torn or whole.

### L6 — adapters

Each adapter is tested for its own job and nothing more.

- `adapter/driver/stdio` — the Unix filter: both framings round trip, a blank line is an empty
  entry, a cut stream keeps what was whole at either batch boundary, a failed write cancels the
  read and drains it, a follow sees what arrives during it, the batch size decides how many
  writes a stream becomes and not what bytes come out, a slow stream is written without
  waiting for a batch, and the line reader's edges.
- `adapter/driver/cli` — flag validation (including the largest frame the format allows and the
  first past it), read-argument parsing, the filter commands, user mistakes reported as one
  line with no source position, and `tools read-log` reading a compressed block.
- `adapter/driver/grpcapi` — the handler against a fake stream (a failed `Send` or a departed
  client stops the read rather than hanging it), TLS at relative paths, and a dial to an
  unreachable address being an error rather than a client.
- `adapter/driven/compression/zstd` — round trips at every level, the append and no-retain
  contracts, an unknown level refused, concurrent frames, an oversized frame refused before it
  is decoded, and a real topic through the codec: smaller, readable from every offset,
  reloadable, and readable by a build without zstd for its plain frames.
- `adapter/driven/locking` — the file lease on real directories: renewal, release, a stalled
  holder not stealing its lease back, a lease that aged out treated as lost, loss reported,
  release not removing a lock it no longer holds, one claimant for a fresh lock and for an
  expired one, a claim never read half-written, a live lease never taken.
- `adapter/driven/logging/zerologger` — level mapping, every field kind, `Enabled` agreeing with
  what is emitted.
- `wiring` — the composition roots: the lease built and refused, shutdown closing the log
  before releasing the lock, in-memory mode touching no filesystem, read-only mode building a
  store that cannot write and an open that leaves the directory byte for byte as it was,
  compression names, codecs and levels, and the embedded root taking only what it is given.

### L7 — end to end

`adapter/driver/grpcapi/test`: each test starts its own server on a free port with
`startTestServer(t)`, which fails the test if the server does not start and stops it on
cleanup, and talks to it with the real client. Listing, a large object, write-then-verify, a
read from every offset across several blocks, and a simulated user. The same package holds the
gRPC benchmarks.

### L8 — system tests against a history

`adapter/driver/history` is where the log is tested as a whole under failure, the way Jepsen
tests a database: clients record what they were told, a fault injector breaks the storage, and
a checker decides whether any single log could have told them all of it. Three parts, each
with a section below: the checker (§6), the randomised nemesis test (§7) and the exhaustive
crash at every storage call (§8).

## 5. The fault models

Both fault filesystems implement `filestore.FS`, the seam below the filesystem store. It is
seven methods and a file handle — exactly the calls the store makes — and `*os.File` already
satisfies the handle. Neither is a filesystem abstraction; each is the real filesystem with one
thing changed.

| | `faultfs.CrashFiles` | `faultfs.PageCache` |
|---|---|---|
| models | a process dying mid-write | the machine losing power |
| what is lost | the rest of one torn write, and every call after | everything not synced, as a `Loss` decides |
| names | as the real filesystem has them | durable only once the directory is synced |
| failed fsync | — | Linux semantics: data dropped, still readable, never written |
| aimed by | `ArmAfterFor(suffix, bytes)` | `CrashAtCall(n)`, `FailAtCall(n)`, `FailSyncs(suffix, n)`, `Crash()` |
| after | `Restart()` | `Recover(loss)` rewrites the directory to what survived |

### `CrashFiles`

Lets one write land partly on the media and then fails every call, as a filesystem does for a
process that is no longer running. Use it where the question is "what does the code make of a
torn write", which a process crash produces and recovery has to repair.

### `PageCache`

LazyFS on this seam. Every call happens on a real directory, and beside it the model keeps
what the media holds:

- **A file's data is durable once that file is synced.** Until then a crash keeps whatever the
  `Loss` lets through: some prefix of the writes and truncates since the last sync, the last
  write possibly torn, nothing after.
- **A name is durable once its directory is synced**, strict POSIX. A file or directory created
  or removed since its parent was synced may or may not be there afterwards, whatever was
  synced inside it. ext4 is kinder in practice, since its fsync commits the journal the create
  is in; the model is not, because nothing promises that.
- **A failed fsync loses what it was asked to write**, as Linux has since 4.13: the pages are
  marked clean and dropped from writeback, so they stay readable from the cache, a later fsync
  succeeds without writing them, and after a crash the range reads as zeros.
- **`DropCache` evicts exactly what a failed fsync dropped**, so the next read of it comes from
  the media as zeros. It is not a media change and is not traced.

The losses a `Recover` can apply:

| loss | keeps of what was not synced |
|---|---|
| `LoseEverything()` | nothing — the harshest crash, and the one every acknowledged write must survive |
| `LoseSome(seed)` | a random prefix of each file's writes, the next one torn at a random byte, each unsynced name change with even odds |
| `KeepBytes(func(path, unsynced) int)` | exactly the bytes the function says, per file, and every name change — for aiming one particular crash |

For the crash at every call (§8) it also traces the calls that change the media (`Trace`,
filtered by `CountOnly`) and answers `DurableContains(root, bytes)`: are these bytes on the
media in some file whose name is durable, every directory up to the root included.

What the model does **not** cover: reordering between files beyond what a `Loss` chooses, a
disk that lies about flushing, a torn page inside a range that was synced, and anything the
kernel does that the model has not been told about. It is a model; it is only as right as the
rules above, and those rules are pinned one by one in `faultfs/pageCache_test.go`.

## 6. The history checker

`adapter/driver/history` is stdlib-only and drives the log only through `driver.LogManager`, so
a history can be taken against the manager, the embedded root or a server behind a client.

**Recording.** `History.Write` and `History.Read` wrap the port and record each operation with
an invocation and a completion on a logical clock (an atomic counter, not wall time): a write
with the entries it sent and the error it got, a read with the offset it asked for and every
entry it was given. Entries must be unique across a history, because the port returns no
offsets and an entry is recognised by its bytes. The workloads write entries like
`p3-w17-e2` or `<A-9-0>`.

**Checking.** `Check(ops, final)` takes the history and the log read back after the run, topic
by topic. A log has one order, so there is nothing to search: the log read back *is* the
order, and every rule is a scan of it.

| anomaly | meaning |
|---|---|
| `offsets not contiguous` | position *i* of the log does not hold offset *i* |
| `entry nobody wrote` | the log holds bytes no client sent |
| `entry written once, in the log twice` | a duplicate |
| `acknowledged write lost` | a write that returned `nil` is not all there |
| `write not whole, or not in order` | what is there of a write is not a contiguous prefix of it |
| `write before an earlier acknowledged write` | real-time order broken: a write acknowledged before another was invoked is after it in the log |
| `read saw what the log does not hold` | a reader was given an entry that is not at that offset afterwards — the durability promise from the reader's side |
| `read not contiguous from where it asked` | a read skipped or repeated an offset |

**A failed write is indeterminate, not failed.** Ibsen does not promise that a write returning
an error left nothing behind — a write is appended as frames, and recovery truncates at the
first damaged one — so the checker allows any prefix of a failed write, at most once, and
nothing else of it.

**Testing the checker.** `history_test.go` gives it every anomaly in a hand-built history, and
the valid histories it must accept: concurrent writes in either order, a failed write that left
nothing, a prefix or all of itself, reads of what is there and empty reads. A checker that
never finds anything proves nothing.

## 7. The nemesis test

`TestNemesis` (`adapter/driver/history/nemesis_test.go`) is the randomised half: four writers
and two readers on two topics, through the manager, three lives each ended by a power cut,
twelve seeds per scenario.

| scenario | loss at each crash | other trouble |
|---|---|---|
| power cut loses everything unsynced | `LoseEverything` | |
| power cut keeps some of what was unsynced | `LoseSome(seed)` | |
| power cut under coalesced flushes | `LoseSome(seed)` | `FlushEntries 20`, `FlushInterval 1ms` |
| fsync fails, then power cut | `LoseEverything` | one log fsync fails at a random point of each life |

Blocks are 2000 bytes and frames three entries, so a life rolls over several times and most
writes are several frames. The crash is brought by whichever writer reaches a random **write**
count — reads are not counted, since an empty read is so quick the readers would use up the
budget before a write finished — so every other client is wherever it is. After the last life
the directory is opened as a restarted server would open it, on the real filesystem, and
everything goes to the checker. It takes about four seconds under `-race`.

**Reading a failure.** A failure names the seed, the number of anomalies, the first one and a
count per kind:

```
seed 7: 1 anomalies, first acknowledged write lost in topic-0: write by process 201,
acknowledged at 607, has 0 of its 2 entries in the log (1× acknowledged write lost)
```

Process numbers are `life*100 + n` for writers and `life*100 + 50 + n` for readers, so `201` is
the second writer of the third life. The clock values place the operation in the history.

**Narrowing a failure down.** Randomised failures are usually several causes at once. What
worked when this test first ran was to remove one suspected cause at a time from the
*environment* and see which anomalies disappear — pre-creating the topic directories durably
before life 0 took away every lost-topic anomaly, and deleting every `.idx` after each
recovery showed the remaining skips were not the index — and then to write the narrowest
deterministic test that reproduces what was left. The nemesis test finds bugs; the
deterministic tests in L4 and L5 pin them.

It is not a proof of anything, and it says so: a bug that needs one particular interleaving in
a few hundred seed runs shows up in a few hundred seed runs. The fifth bug it found did
exactly that (§12).

## 8. A crash at every storage call

`TestCrashAtEveryStorageCall` (`adapter/driver/history/crashpoints_test.go`) is the exhaustive
half, after Molly's lineage-driven fault injection: ask what each good outcome rested on, and
whether taking that away breaks it.

### Lineage, checked at the promise

Whenever the workload is promised something — a write acknowledged, an entry handed to a
reader — the test asks `PageCache.DurableContains` whether those bytes are on the media under
names durable all the way to the root. **A promise without that support fails the run it is
made in**, crash or no crash, and does so deterministically, on the first run that makes the
promise. With the root-directory sync of step 31 removed, the fault-free run fails on its
first acknowledged write.

### Every call, by brute force

One deterministic workload — two topics, 300-byte blocks and three-entry frames, a read after
every third write, a restart of the process halfway, and four more writes in a new life after
any crash — is traced once without faults. Then:

- **for every call in the trace**, the workload runs again with the power cut as that call
  begins, under each of four losses, recovers, writes on in the new life, and hands the whole
  history to the checker;
- **for every sync in the trace**, the workload runs again with that sync failing, the power
  cut at the end, and the same recovery and check.

| loss | stands for |
|---|---|
| power cut | `LoseEverything` |
| process crash | `KeepBytes(all)`: the kernel kept everything it was given |
| power cut keeping some | `LoseSome(point)` |
| **writeback newest first** | the newest block of each topic reached the media, and the unsynced tails of the blocks before it did not |

The last loss is the lineage-driven one: it is aimed at what a rollover rests on, which is the
old block being whole before the new one exists. **It is the only loss that catches the
rollover barrier being removed**; the three generic ones pass.

The space is small enough to enumerate rather than prune: 67 calls, 268 crash runs and 38
failed-sync runs, about nine seconds under `-race`.

### Pairs

A lone writer never finds another write's tail unsynced, since each of its writes is durable
before the next begins, so a sequential workload cannot reach the state a rollover has to be
safe in — and the first version of this harness passed with the rollover barrier removed.
Every fourth write is therefore a pair: the first is held at its flush by `gatedStore`, which
arms to hold syncs only *after* the next log append (so it never holds a rollover of the first
write's own), and the second, sized to roll the block over now and then, arrives beside it.

The second write gets 5 ms to append beside the held flush before the gate opens. With the
barrier in place it cannot append — it waits for the old block inside the flusher — and the
trace comes out the same whether it got there in time or not, because the barrier's sync runs
after the held one either way. Without the barrier, those 5 ms are what lets it append ahead
of the old block's tail, which is the state the targeted loss then breaks.

### Determinism

Naming a crash by its call number needs the same calls in the same order every run:

- **Only calls outside `.idx` files are counted.** The indexer writes them on its own
  schedule, and nothing is promised on the index, which is derived and never synced.
- **The flusher syncs a batch's blocks oldest first** (`flusher.syncBlocks`), not in map order.
- **The test checks itself.** Two fault-free runs must give the same trace, and every crash run
  must reach the call it was aimed at; either failing is reported as a broken workload, not as
  a bug in the log.

A change that makes the log's calls on log blocks or directories depend on scheduling will
fail here first. Fix the nondeterminism, or count the calls differently — do not loosen the
check.

### Reading a failure

```
writeback newest first at call 37 (write /topic-0/00000000000000000026.log): 2 problems,
first: offsets not contiguous in topic-0: position 23 holds offset 26
```

The loss, the call number, the call itself and the first problem. Problems are checker
anomalies, the log failing to read back, or an unsupported promise such as `acknowledged
<A-5-0> at call 23`. Up to twenty failing points are reported and the rest counted.

### What it catches, and what it does not

Checked by reverting fixes one at a time:

| reverted | caught by |
|---|---|
| step 31, the root-directory sync of a new topic | the lineage check, in the fault-free run |
| step 34, the rollover barrier | the writeback-newest-first loss, at the two calls around the rollover |
| step 35, returning a failed directory sync | every loss, at the directory-sync call |
| step 37, confirming names once per process | the failed-sync runs, at directory syncs before the restart |
| step 38, dropping the head block from the cache | the failed-sync runs, at log syncs before the restart |

It does **not** reach step 32's read skip, which needs a flush to land inside a read; the
nemesis test and `readAcrossFlush_test.go` hold that. A second crash during the recovery of the
first is not enumerated.

## 9. Running the suite

| what | command |
|---|---|
| everything CI runs | `gofmt -l . && go vet ./... && ./scripts/check-architecture.sh && ./scripts/embedded-size.sh && go test -race ./...` |
| the whole suite | `go test -race ./...` |
| one package | `go test -race ./core/topic/` |
| one test | `go test -race ./core/topic/ -run TestAFailedFlushStopsTheTopic -count=1 -v` |
| the system harnesses | `go test -race ./adapter/driver/history/ -count=1 -v` |
| a flaky test, hard | `go test -race ./core/topic/ -run TestX -count=50` |
| a hang, with a goroutine dump | `go test ./core/topic/ -run TestX -timeout 20s` |

`-count=1` defeats the test cache. On a two-core machine the whole suite takes about 105
seconds under `-race`, and three packages are nearly all of it: `core/topic` (≈66 s, mostly the
property tests), `adapter/driver/history` (≈23 s) and `adapter/driver/grpcapi/test` (≈22 s).

**A hang is a test waiting on a gate nobody opened, or a cleanup closing a manager whose writes
are blocked.** A short `-timeout` turns it into a goroutine dump; look for the test's own
frames and for `chan receive` in a gated `Sync`. A test that holds syncs must release them in a
`t.Cleanup` registered *after* the manager's, so it runs first and a failing test does not hang
its own cleanup.

### Benchmarks

Not run by CI. Each says what it measures in its comment, and the results live in
ARCHITECTURE.md and CLAUDE.md.

| benchmark | measures | run |
|---|---|---|
| `BenchmarkFrameWrite`, `FrameReadOne`, `FrameReadAll` | the frame bound against compression ratio and read cost | `go test ./adapter/driven/compression/zstd/ -run XXX -bench BenchmarkFrame` |
| `BenchmarkGrpcWrite`, `GrpcReadAll`, `GrpcReadOne` | a write and a read through gRPC at the defaults, on disk | `go test ./adapter/driver/grpcapi/test/ -run XXX -bench . -benchtime 1s` |
| `BenchmarkGrpcWriteFlushPolicy` | fsyncs per write under the flush policy, eight concurrent clients | as above, `-bench FlushPolicy -benchtime 2s` |
| `BenchmarkFindNearestByteOffset` | index lookup | `go test ./core/index/ -run XXX -bench .` |

**Where `TMPDIR` points decides the write numbers**, because the default policy is an fsync
per write: on tmpfs a benchmark measures the code, on a disk it measures the disk. Use
`TMPDIR=/var/tmp` or somewhere on the media a deployment would use.

## 10. Recipes

### Fixing a bug

1. **Reproduce it at the narrowest level that can see it.** A format bug is an L1 test, a
   contract bug an L4 test with a gate, a crash bug an L5 test with the fault model. A nemesis
   failure is a lead, not a reproduction.
2. **Watch the test fail for the reason it states.** Not a timeout, not a different error.
3. Fix it.
4. **Stash the fix and see the test fail again; restore it and see it pass**, under `-race`
   and with `-count` high enough to trust a concurrent test.
5. Run the system harnesses. If the bug was in their reach and they did not catch it, the
   harness has a gap: close it, and check that it now catches the bug.
6. Say in the test's comment what used to happen and what found it.

### Adding a storage adapter

1. Implement `driven.BlockStore`. Implement `driven.Syncable` if the medium has a volatile
   buffer, and only then.
2. Run `conformance.Run` against it in its own package.
3. Add it to `coreBackends()` so the core's property tests run on it.
4. If it is stdlib-only, add it to rule 1 of `scripts/check-architecture.sh`.

### Writing a `BlockStore` decorator

Forward `Syncable`. It is probed by type assertion, so a wrapper without `Sync` makes the core
conclude the store has nothing to flush, and every write becomes readable the moment its
append returns — nothing fails, the durability promise just stops holding. Every gated store in
the test suite forwards it for the same reason. Keep `Append` all or nothing, and wrap a
failure that cannot be rolled back in `driven.ErrDirtyBlock`.

### Changing anything on the durability path

The flusher, recovery, rollover, the store's `Sync`, or the order of calls a write makes:

- the L4 flush tests and the L5 power-cut tests must pass;
- `TestCrashAtEveryStorageCall` must pass, and stay deterministic. If a new call pattern is
  something its workload never does — a new kind of block, a new directory — extend the
  workload so it does, and check the harness catches the change being reverted;
- `TestNemesis` must pass, ten times over (`-count=10`) for anything that touches concurrency.

### Changing a format

Every byte of a header must stay covered by a checksum, and `frame_test.go` checks each one.
Add the old bytes as a fixture before changing the code, and decide explicitly whether the old
format is read, rebuilt (as the index is) or reported (as a block written before framing is).
Never truncate bytes the build does not understand.

### Adding a driving adapter

Drive `driver.LogManager` and nothing else; rule 3 of the architecture check enforces it. Test
it against a real manager on `memstore` or a real directory. If it can be stdlib-only, put it
in rule 1.

## 11. Promises and the tests that hold them

| promise | held by |
|---|---|
| an acknowledged write is durable | `flush_test.go`; `powerCut_test.go` (topic and store); `TestNemesis`; `TestCrashAtEveryStorageCall` lineage check and checker |
| nothing is readable before it is durable | `TestReadersDoNotSeeAnUnflushedEntry`; the checker's `read saw what the log does not hold`; the lineage check on reads |
| a failed fsync is not retried | `TestAFailedFlushStopsTheTopic`, `TestABatchBehindAFailedFlushFailsWithIt`, `TestAFailedRolloverSyncStopsTheTopic`, `TestPowerCut_aFailedFsyncIsNotRetryable`, the failed-sync runs |
| a restart does not trust a failed fsync | `TestPowerCut_aRestartAfterAFailedFsyncDoesNotWriteBehindTheHole`, `TestPowerCut_aRestartRetriesTheDirectorySyncThatFailed`, the failed-sync runs before the restart |
| a name is durable before a write in it is acknowledged | `TestPowerCut_aSyncedBlockInANewTopicSurvives`, `TestPowerCut_aSyncCutBeforeTheDirectoryIsNotDurable`, `dirsync_test.go`, the lineage check |
| a write is whole or a prefix | `TestAWriteIsOneAppendHoweverManyFramesItMakes`; `filestore/crash_test.go`; the checker's `write not whole` |
| offsets are contiguous | the property tests; `TestPowerCut_aRolloverWaitsForTheOldBlock`; the checker's `offsets not contiguous` |
| a read does not skip | `TestReadDoesNotSkipTheEndOfABlockWhenAFlushLandsDuringIt`; the property tests; the checker's `read not contiguous` |
| recovery never destroys what is not damaged | `logUtils_test.go` (pre-framing reported, offset gap an error, unwired codec recovered); `indexChecksum_test.go` |
| one writer | `locking/singleInstanceLock_test.go`; `wiring/lock_test.go`; `wiring/local_test.go` |
| a reader changes nothing | `TestReadOnlyOpenWritesNothing`; `TestReadonlyModeBuildsAStoreThatCannotWrite`; `TestList_aReadOnlyStoreKeepsTheCache` |
| the core is pure | `scripts/check-architecture.sh`; `scripts/embedded-size.sh` |

## 12. What the harnesses have found

The fault model and the two system harnesses have found seven bugs in the log, every one of
which the rest of the suite passed with. Each is fixed and pinned by a deterministic test;
CLAUDE.md §12 has the full account.

| found by | bug |
|---|---|
| nemesis | a new topic's directory was never made durable, so a power cut could take a whole topic |
| nemesis | a read spanning two blocks skipped the end of the first when a flush landed during it — no crash needed |
| nemesis | a failed fsync was retried, and under Linux semantics the retry acknowledged writes behind a hole |
| nemesis | a rollover could leave the block before the head short or torn, which recovery never looks at |
| nemesis | a failed directory sync was swallowed, acknowledging a write into a block a power cut took |
| crash at every call | a restart forgot that a name's directory sync had failed, and never retried it |
| crash at every call | a restart read the bytes a failed fsync dropped back from the page cache and wrote behind the hole |

Two more were caught on the way, by the habits of §2 rather than by a harness: the first
version of the rollover fix let a flush returning just after a failed rollover sync be
acknowledged, which its own test (`TestAFailedRolloverSyncStopsTheTopic`) caught before it was
committed; and the first crash-at-every-call workload could not reach a rollover with an
unsynced tail at all, which reverting the rollover fix and watching the harness pass revealed.

## 13. Known gaps

What the suite does not test, so nobody assumes it does:

- **Clients that retry.** Every harness drives the manager. A gRPC client that retries a write
  would duplicate it, since writes carry no idempotency key, and the checker would say so — but
  nothing exercises it.
- **The lease under failure.** The lease's own tests cover stalls and races between instances;
  no history-checked test runs two writers against one directory with a nemesis.
- **Nested crashes.** A second crash during the recovery from the first is not enumerated.
- **Eviction off Linux.** `DropCache` is a no-op on other platforms and advice on Linux; the
  model treats it as exact.
- **What the model does not model** (§5): a disk that lies about flushing, torn pages inside a
  synced range, and reordering the losses do not choose.
- **Real power loss and real hardware.** Everything above is simulated in-process. A
  power-cycling rig, or LazyFS under a real process, would test the model as well as the log.

---

## Appendix: catalogue of test files

| file | level | what it pins |
|---|---|---|
| `logcalls_test.go` | L0 | every zerolog event in production code is sent |
| `core/domain/frame_test.go` | L1 | frame header round trip, every byte checksummed, magic, version, bounds |
| `core/domain/fsUtils_test.go` | L1 | entry codec, its failures, zero allocations, aliasing |
| `core/domain/accessDomain_test.go` | L1 | domain helpers |
| `core/domain/race_test.go`, `norace_test.go` | — | build-tag pair telling the allocation pin whether `-race` is on |
| `core/index/*_test.go` | L1 | checksummed pairs, lookup against the linear scan, building, sparsity |
| `core/logfmt/logUtils_test.go` | L1 | recovery and reads against every kind of damaged block |
| `core/logfmt/appendFrame_test.go` | L1 | building a frame into the block buffer |
| `core/logfmt/fallback_test.go` | L1 | compression that does not pay is stored plain |
| `core/port/driven/codec_test.go` | L1 | the codec registry |
| `errore/errorHandler_test.go` | L1 | the error helper |
| `adapter/driven/blockstore/conformance/` | L2 | the suite every store runs |
| `adapter/driven/blockstore/memstore/memStore_test.go` | L2 | conformance on memory |
| `adapter/driven/blockstore/flashstore/flashStore_test.go` | L2 | conformance on flash, page table rebuild, reclaim, full region, write-once bits |
| `adapter/driven/blockstore/filestore/conformance_test.go` | L2 | conformance on a real directory |
| `core/topic/topicAccess_property_test.go` | L3 | every offset, every store, every reload mode; concurrent write, read and index |
| `core/topic/topicAccess_test.go`, `topicAccess_recovery_test.go` | L3 | the topic's write, read, load and recovery paths |
| `core/topic/frame_test.go`, `sparsity_test.go`, `indexChecksum_test.go`, `logging_test.go` | L3 | frames per write, sparsity, index rebuild, logging through the port |
| `core/topic/flush_test.go` | L4 | the durability contract and fail-stop |
| `core/topic/readAcrossFlush_test.go` | L4 | a read across a flush does not skip |
| `core/topic/indexing_test.go` | L4 | indexing coalesces |
| `core/manager/*_test.go` | L4 | one load per topic, close, shared flushes, topic names, frame bounds |
| `adapter/driven/blockstore/faultfs/pageCache_test.go` | L5 | every rule of the power-cut model, and conformance through it |
| `adapter/driven/blockstore/filestore/crash_test.go` | L5 | torn writes below the store |
| `adapter/driven/blockstore/filestore/powerCut_test.go` | L5 | names and restarts under power cuts |
| `adapter/driven/blockstore/filestore/dirsync_test.go`, `durability_test.go`, `evict_test.go` | L5 | what `Sync` and `List` do to the disk and the cache |
| `adapter/driven/blockstore/filestore/readonly_test.go` | L5 | the read-only filesystem refuses changes |
| `core/topic/topicAccess_crash_test.go` | L5 | the core recovering from torn writes |
| `core/topic/powerCut_test.go` | L5 | the core under power cuts, failed fsyncs and restarts |
| `adapter/driver/stdio/stdio_test.go` | L6 | the Unix filter |
| `adapter/driver/cli/*_test.go` | L6 | flags, arguments, filter commands, user errors, tools |
| `adapter/driver/grpcapi/*_test.go` | L6 | the handler, TLS, dialling |
| `adapter/driven/compression/zstd/*_test.go` | L6 | the codec, and a topic through it |
| `adapter/driven/locking/singleInstanceLock_test.go` | L6 | the file lease |
| `adapter/driven/logging/zerologger/zerologger_test.go` | L6 | the logging adapter |
| `wiring/*_test.go`, `wiring/embedded/embedded_test.go` | L6 | the composition roots |
| `adapter/driver/grpcapi/test/` | L7 | a real server on a free port; gRPC benchmarks |
| `adapter/driver/history/history_test.go` | L8 | the checker, against hand-built histories |
| `adapter/driver/history/nemesis_test.go` | L8 | randomised power cuts and failed fsyncs, checked |
| `adapter/driver/history/crashpoints_test.go` | L8 | a crash at every storage call, and every promise's support |
| `adapter/driven/compression/zstd/bench_test.go`, `core/index/find_test.go` | bench | frame bound and index lookup |

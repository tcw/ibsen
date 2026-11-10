# User guide

This guide is for people **using** Ibsen: writing to a log, reading it back, keeping it on
disk, linking it into a Go program, and talking to the server. [OPERATIONS.md](OPERATIONS.md)
is for people **running** it: flags, durability, compression, TLS and the rest.
[The README](../README.md) is the introduction, and explains what a log is if you are new to
the idea.

- [Words used here](#words-used-here)
- [Keep the data](#keep-the-data)
- [Use it as a filter](#use-it-as-a-filter)
- [Embed it](#embed-it)
- [Safe retries](#safe-retries)
- [The API](#the-api)

---

## Words used here

- A **topic** is one named log, such as `greetings` or `audit`. A data directory holds many.
- An **entry** is one item in a topic: any bytes, often one line of text.
- An **offset** is an entry's number within its topic, starting at 0. Offsets never change and
  are never reused.
- A **block** is one file a topic's entries are stored in. A topic grows a new block when the
  current one is full.

## Keep the data

Point the server at a directory and the log is on disk, laid out as one directory per topic:

```shell
mkdir -p /tmp/ibsen/data
ibsen server -d /tmp/ibsen/data
```

```
/tmp/ibsen/data/
├── .writeLock                    single-writer lock, so a second server cannot write here
└── greetings/
    ├── 00000000000000000000.log  entries, framed and checksummed
    └── 00000000000000000000.idx  sparse offset → byte offset index
```

Blocks are named by the offset of their first entry, and roll over at `--maxBlockSize` MB.
The index is derived from the log and is rebuilt whenever it disagrees with it, so it is
never the thing you have to protect. An entry is not readable until it is on durable media;
when a write is acknowledged depends on the flush policy, described in
[OPERATIONS.md](OPERATIONS.md#durability).

You can look at the files without a server:

```shell
ibsen tools read-log   /tmp/ibsen/data/greetings/00000000000000000000.log
ibsen tools read-index /tmp/ibsen/data/greetings/00000000000000000000.idx
```

The byte layout of both files is in [ARCHITECTURE.md §5](../ARCHITECTURE.md#5-storage-formats).

## Use it as a filter

The same log, driven over stdin and stdout. No daemon, no socket: `append` and `cat` open the
data directory themselves.

```shell
printf 'hello\nworld\n' | ibsen append -d /tmp/ibsen/data greetings
ibsen cat -d /tmp/ibsen/data greetings
ibsen cat -d /tmp/ibsen/data greetings 1 --offsets   # from offset 1, with offsets
ibsen cat -d /tmp/ibsen/data greetings --follow      # keep printing as entries arrive
ibsen topics -d /tmp/ibsen/data
```

Entries come out exactly as they went in, so a topic pipes into anything, including another
topic:

```shell
ibsen cat -d /tmp/ibsen/data greetings | grep world | ibsen append -d /tmp/ibsen/data hits
```

One entry per line by default. An entry that holds a newline, or no bytes at all, needs
`--framing length`, which prefixes each entry with its byte count as a little-endian uint64 —
readable and writable by both commands, and nothing to do with the format on disk.

**`cat` opens the log read-only and takes no lock**, so it is safe to point at a directory a
server is writing: it cannot truncate, re-index or create anything. **`append` takes the
single-writer lock**, so it is refused while a server holds that directory — two writers on
one log is what the lock exists to prevent. It takes `--lock` like the server; see
[Choosing a lock](OPERATIONS.md#choosing-a-lock).

### The filter commands' flags

`append`, `cat` and `topics` take `-d, --rootDirectory` (or `IBSEN_ROOT_DIRECTORY`) like the
server, and there is no in-memory fallback for them: a log that lives in one process and is
shared with nobody has nothing to append to. `append` also takes every write-side server flag
listed in [OPERATIONS.md](OPERATIONS.md#configuration) — `--maxBlockSize`, the flush flags,
`--indexSparsity`, `--maxFrameEntries`, `--maxFrameBytes`, `--compression`,
`--compressionLevel`, the retention flags and `--config` — because those describe what is written, and a filter writes the same
log a server does. For an `append`, `--batchSize` is what moves the throughput; see
[Durability](OPERATIONS.md#durability).

| flag | commands | default | what it does |
|---|---|---|---|
| `--framing` | `append`, `cat` | `lines` | how the stream delimits entries: `lines`, or `length` for a little-endian uint64 byte count before each entry |
| `--fromEarliest` | `cat` | `false` | with an offset, start at the earliest retained entry if that offset was deleted; with none, `cat` always does |
| `--batchSize` | `append`, `cat` | `10000` / `1000` | entries per write, or per read; for `append` this is one flush, so it is the throughput dial |
| `--offsets` | `cat` | `false` | prefix each entry with its offset and a tab; line framing only |
| `-F, --follow` | `cat` | `false` | keep printing as entries are written, until Ctrl-C |
| `--pollMs` | `cat` | `1000` | milliseconds between passes while following |

Following reopens the log on every pass, which is what lets it see entries another process
appends. That costs a topic load per pass, and a load scans the head block, so a large head
block wants a larger `--pollMs`. Nothing is written by any of it — `cat` opens the log
read-only.

`--offsets` with `--framing length` is refused rather than ignored: length framing carries
entries and nothing else, so a reader of it needs to be told nothing.

### Starting from the earliest offset

A data directory may have retention set (see [Retention](OPERATIONS.md#retention)), which
deletes a topic's oldest entries. Offsets never shift when that happens: the entry at offset
290000 is still at 290000, and the ones before it are simply gone.

- `ibsen cat topic` with no offset starts at the earliest entry still there.
- `ibsen cat topic 0` asks for offset 0 exactly, and fails with a message naming the earliest
  offset if it has been deleted. Add `--fromEarliest` to start there instead.
- Over gRPC, a read of a deleted offset fails with `OUT_OF_RANGE`; set `fromEarliest` in
  `ReadParams` to start at the earliest. `ibsen client read --fromEarliest` does.
- Embedded, `driver.ReadParams{FromEarliest: true}`; without it the read returns an error
  matching `domain.ErrOffsetTooOld`, whose `*domain.OffsetTooOldError` names the earliest
  offset.

A reader that is following the log and falls behind until the next block it needs is deleted
gets the same error, not a silent jump forward.

## Embed it

The gRPC server is an adapter around the log, not the log. Import the embedded composition
root and you get the same core in your own process, through the same interface the server
speaks:

```go
package main

import (
	"fmt"
	"sync"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/wiring/embedded"
)

func main() {
	log, err := embedded.Open(embedded.Params{Store: filestore.NewOS("/tmp/ibsen/data")})
	if err != nil {
		panic(err)
	}
	defer log.Close()

	entries := [][]byte{[]byte("hello"), []byte("world")}
	if _, err := log.Write(driver.WriteParams{Topic: "greetings", Entries: &entries}); err != nil {
		panic(err)
	}

	batches := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	go func() {
		for batch := range batches {
			for _, entry := range *batch {
				fmt.Printf("%d\t%s\n", entry.Offset, entry.Entry)
			}
			wg.Done()
		}
	}()
	err = log.Read(driver.ReadParams{TopicName: "greetings", LogChan: batches, Wg: &wg, BatchSize: 10})
	wg.Wait()
	close(batches)
	if err != nil {
		panic(err)
	}
}
```

A runnable version of this is in
[`wiring/embedded/example`](../wiring/embedded/example/main.go).

An embedded log is strict by default: `Write` returns once the entries are on durable media.
`embedded.Params{FlushMode: topic.FlushLenient}` acknowledges a write once it is appended
instead, and nothing is durable or readable until it is flushed. Set `FlushInterval` and
`Open` starts a ticker that calls `Flush` that often, or leave it zero and call `log.Flush()`
yourself. `Close` flushes either way, and returns the error if that flush failed. See
[Durability](OPERATIONS.md#durability) for what each mode risks.

### Swapping stores

Swap `filestore.NewOS(dir)` for `memstore.New()` to keep it in RAM, or for `flashstore` to
put it on a raw flash region with no filesystem underneath. That is the only line that
changes.

Storage is a port with six block verbs — list, append, open at a byte offset, truncate,
remove, plus creating and enumerating topics — and no filesystem concepts at all. That is why
one core can sit on:

| adapter | what it is | for |
|---|---|---|
| `filestore` | numbered files under a directory, standard library only | servers, laptops, containers |
| `memstore` | byte slices in RAM | tests, caches, a log nobody needs to keep |
| `flashstore` | a fixed region of raw flash: fixed pages, write-once bytes, page table in RAM | microcontrollers, no filesystem |

Every one of them passes the same conformance suite, which is the only reason to trust the
exotic ones. Compression, logging, telemetry, transport and the write lock are ports the same
way, chosen where the program is wired rather than inside the log.

### What an embedded build links

**You link what you bring.** An embedded build that wires one of those stores stays inside
the Go standard library: no gRPC, no cobra, no OpenTelemetry, no logging framework, no
compressor. That is not a promise in a README, it is checked on every push by
[`scripts/check-architecture.sh`](../scripts/check-architecture.sh) and weighed by
[`scripts/embedded-size.sh`](../scripts/embedded-size.sh):

| target | full server | embedded |
|---|---|---|
| linux/amd64 | 16.4 MB | 1.87 MB |
| linux/arm | 15.4 MB | 1.81 MB |

An embedded build defaults to no compression, because linking zstd is what those numbers
measure the absence of. A directory written by the server with its default (zstd) can only be
read by an embedded build that wires the zstd adapter too.

## Safe retries

A write can succeed on the server and still look like a failure to the client: the reply is
lost to a timeout, a dropped connection or a server restart. The client cannot tell whether
the entries were stored, and if it simply sends them again they may be stored twice.

Safe retries fix this. The client picks a **producer ID** — 16 random bytes, written as a UUID
— and numbers its writes to each topic 1, 2, 3. If the reply to write 7 is lost, the client
sends write 7 again with the same number. The server remembers the last number it accepted
from each producer: if it already has write 7, it stores nothing and answers "already
written", with the offsets the original went to. The ID and the number are saved in the log
itself, so this still works after the server restarts.

The rules, for a client that wants to know exactly:

- The same number as the last one accepted is a duplicate: success, original offsets, nothing
  stored. The next number is a normal write.
- A gap, or an older number, is refused (`FAILED_PRECONDITION` over gRPC). It means the client
  numbered something wrong, and is caught rather than hidden.
- The first write from a producer the server does not know may carry any number.
- Writes with no producer ID behave exactly as before, so old clients keep working.

**In Go**, the client in [`clients/go/ibsen`](../clients/go/ibsen/client.go) does all of it:

```go
client, err := ibsen.Dial("localhost:54321", []ibsen.Option{ibsen.WithProducer()})
if err != nil {
	panic(err)
}
defer client.Close()
result, err := client.Write(ctx, "orders", [][]byte{[]byte(`{"id":1}`)})
// a lost reply was retried with the same number; result.Duplicate says the server already had it
```

**From the shell**, give `ibsen append` a producer ID and rerunning a pipeline that failed half
way is safe — the batches the topic already holds are skipped:

```shell
ibsen append -d /srv/ibsen orders --producerId 3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90 < orders.ndjson
```

Use one ID per input. With a producer ID a batch is written only when it is full or the input
ends — never because the input paused — so that a rerun cuts the input into the same batches
and gives them the same numbers.

**The forgetting window.** The server remembers a producer only while its last write is in one
of the topic's newest `--producerWindowBlocks` blocks (default 4, and a block is up to
`--maxBlockSize`, a gigabyte by default). A producer that has written nothing for longer is
treated as new: its next write is accepted whatever its number, and a retry of a write that
old would be stored again. Retry promptly, or raise the window.

**With lenient flushing** (the default, see [Durability](OPERATIONS.md#durability)) "already
written" means the original is in the log. After a power cut that lost the original, the
server no longer knows about it, and the retry is simply written — which is what you want.

## The API

Three calls, defined in [`api/grpcApi/ibsen.proto`](../api/grpcApi/ibsen.proto):

```proto
service Ibsen {
  rpc write (InputEntries) returns (WriteStatus);
  rpc read  (ReadParams)   returns (stream OutputEntries);
  rpc list  (EmptyArgs)    returns (TopicList);
}
```

`read` takes a topic, a starting offset, a batch size and `stopOnCompletion`: false keeps the
stream open and follows the log, true ends it when it reaches the end. `fromEarliest` starts
at the earliest retained offset when the one asked for has been deleted.

`write` takes a topic and its entries, and optionally a 16-byte `producerId` and a `sequence`
for [safe retries](#safe-retries). It answers with `wrote`, the `firstOffset` the entries went
to, and `duplicate` when the write was a retry the server already held.

Those three calls are the whole of what Ibsen offers, and gRPC is one way to reach them.
Embedded programs get them as a Go interface, `driver.LogManager`; `ibsen append`, `ibsen cat`
and `ibsen topics` are the same three over stdin and stdout. Nothing in the log knows which
one is driving it.

### Generating clients yourself

```shell
apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc --proto_path=api/grpcApi ibsen.proto --go_out=. --go-grpc_out=.
```

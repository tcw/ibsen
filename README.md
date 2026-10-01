# Ibsen

<img src="mascot/ibsenMascot.svg" width="220" height="220" alt="Ibsen mascot" align="right">

**An append-only log that runs anywhere — SQLite for logs.**

Topics you append entries to and read back by offset. Ibsen is one small Go binary with no
broker to operate, no cluster to form and no ZooKeeper to keep alive, or a library you import
and link straight into your program. The same log core runs on a microcontroller with a chip
of flash and behind gRPC on a replicated Kubernetes volume. Nothing about it changes in
between; only the things bolted to its edges do.

It is a log, not a message queue: entries are durable, ordered and addressed by offset, and a
reader decides for itself where it is. It is under development, and honest about that — see
[Status](#status).

<br clear="right">

---

## Try it in a minute

```shell
go install github.com/tcw/ibsen@latest
```

Start a server with no data directory and the whole log lives in memory — nothing touches a
disk, nothing outlives the process. Good for a first look and for tests:

```shell
ibsen server
```

In another terminal, write a few entries (one entry per line, from stdin or a file) and read
them back:

```shell
printf 'hello\nworld\n' | ibsen client write greetings
ibsen client list
ibsen client read greetings
```

```
0	hello
1	world
```

`read` prints `offset<TAB>entry` and then **follows the log**, printing entries as they are
written, until you stop it with Ctrl-C. To start somewhere other than the beginning, give it
an offset:

```shell
ibsen client read greetings 1
```

## Keep the data

Point the server at a directory and the log is on disk, laid out as one directory per topic:

```shell
mkdir -p /tmp/ibsen/data
ibsen server -d /tmp/ibsen/data
```

```
/tmp/ibsen/data/
├── .writeLock                    single-writer lease, so a second server cannot write here
└── greetings/
    ├── 00000000000000000000.log  entries, framed and checksummed
    └── 00000000000000000000.idx  sparse offset → byte offset index
```

Blocks are named by the offset of their first entry, and roll over at `--maxBlockSize` MB.
The index is derived from the log and is rebuilt whenever it disagrees with it, so it is
never the thing you have to protect. By default every write is on durable media before it is
acknowledged, and it is not readable a moment before that.

You can look at the files without a server:

```shell
ibsen tools read-log   /tmp/ibsen/data/greetings/00000000000000000000.log
ibsen tools read-index /tmp/ibsen/data/greetings/00000000000000000000.idx
```

## Use it as a filter — no server either

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
single-writer lease**, so it is refused while a server holds that directory — two writers on
one log is what the lease exists to prevent.

## Embed it — no server at all

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
	if err := log.Write("greetings", &entries); err != nil {
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

Swap `filestore.NewOS(dir)` for `memstore.New()` to keep it in RAM, or for `flashstore` to
put it on a raw flash region with no filesystem underneath. That is the only line that
changes. A runnable version of this is in
[`wiring/embedded/example`](wiring/embedded/example/main.go).

**You link what you bring.** An embedded build that wires one of those stores stays inside
the Go standard library: no gRPC, no cobra, no OpenTelemetry, no logging framework, no
compressor. That is not a promise in a README, it is checked on every push by
[`scripts/check-architecture.sh`](scripts/check-architecture.sh) and weighed by
[`scripts/embedded-size.sh`](scripts/embedded-size.sh):

| target | full server | embedded |
|---|---|---|
| linux/amd64 | 16.4 MB | 1.87 MB |
| linux/arm | 15.4 MB | 1.81 MB |

---

## Why Ibsen exists

### SQLite for logs

Most of the time you do not need Kafka. You need the *log* — ordered, durable, addressed by
offset, readable from any point by any number of consumers — and you get it with a cluster
attached, plus the operational weight of keeping that cluster healthy, long before your data
is big enough to need any of it.

SQLite made the same observation about databases: for the overwhelming majority of
applications, a database is a file and a library, not a server and a team. Ibsen takes that
seriously for logs.

- **A library first.** The log is an ordinary Go value inside your program. No port, no
  daemon, no connection pool, no serialisation you did not ask for.
- **A command when that is all you need.** `ibsen append` and `ibsen cat` open the data
  directory themselves, so a shell pipeline can write and read a log with nothing running.
- **A server when you want one.** The same core behind gRPC, one static binary, one data
  directory.
- **Small enough to be boring.** The core imports nothing but the parts of the standard
  library TinyGo supports. There is no `os`, no `net` and no global logger in it, so it will
  run where there is no operating system to speak of.
- **The file format is the interface.** Entries and frames carry their own checksums and say
  what they are. The index is derived and disposable; the log is the truth.

### The same log from a microcontroller to a cluster

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

### What Ibsen deliberately does not build

**Ibsen does not implement replication, consensus or distributed locking, and that is the
design, not a gap.**

Raft, Paxos and their friends are not hard to write badly and very hard to write well.
Operating them is harder still, and every log that has grown its own has spent years on it.
The world already has systems whose entire job is to agree on things, and infrastructure
whose entire job is to keep bytes alive on more than one machine. Ibsen's position is that a
log should use them rather than reimplement them:

- **Replication belongs to storage.** Run Ibsen as a Kubernetes StatefulSet on a replicated
  volume, or on whatever your platform already replicates. Those systems are maintained by
  people whose job it is, and they were being hardened long before your log existed.
- **Fencing belongs to whatever already holds elections.** If you need a distributed
  single-writer guarantee, etcd, Kubernetes leases or your cloud's lock service already give
  you one with a monotonic token. Ibsen is built so that token becomes a thin storage
  decorator at wiring time — no change to the core, no change to any port.
- **What Ibsen ships itself is a backstop, not a cluster.** A file lease at
  `<root>/.writeLock` keeps a second server from writing the same directory. It is honest
  about its limits: claiming a free lock is `O_CREATE|O_EXCL`, which a real filesystem makes
  atomic; taking over an expired one is confirmed by re-reading rather than assumed, because
  a filesystem has no compare-and-swap. Renewal proves the lease is still its own before
  extending it, and the moment it cannot, the process stops writing — it does **not** shut
  down cleanly, because a clean shutdown flushes, and flushing is exactly what a writer that
  may have lost its lease must not do.
- **Embedded pays nothing for any of this.** The coordination port has a no-op adapter, so a
  single-process or microcontroller build carries no locking code at all.

The honest version of the trade: Ibsen gives you a very good single-writer log and expects
your platform to provide durability across machines. If you need multi-master writes with
automatic failover and no external system, Ibsen is the wrong tool and Kafka is waiting for
you.

---

## Configuration

Every server flag has an environment variable, so containers need no command line.

| flag | env | default | what it does |
|---|---|---|---|
| `-d, --rootDirectory` | `IBSEN_ROOT_DIRECTORY` | *(unset)* | where the log is kept; unset means in memory |
| `-l, --host` / `-p, --port` | `IBSEN_HOST` / `IBSEN_PORT` | `0.0.0.0` / `54321` | gRPC listener |
| `-m, --maxBlockSize` | `IBSEN_MAX_BLOCK_SIZE` | `1000` | MB per block before rolling over |
| `-f, --flushEntries` | `IBSEN_FLUSH_ENTRIES` | `1` | entries that may wait for a flush before `--flushIntervalMs` is up; on its own it does nothing |
| `--flushIntervalMs` | `IBSEN_FLUSH_INTERVAL_MS` | `0` | how long a batch may wait for company; `0` never waits |
| `-i, --indexSparsity` | `IBSEN_INDEX_SPARSITY` | `10` | entries between index pairs |
| `--maxFrameEntries` | `IBSEN_MAX_FRAME_ENTRIES` | `1000` | entries that may share a frame |
| `--maxFrameBytes` | `IBSEN_MAX_FRAME_BYTES` | `1 MiB` | entry bytes that may share a frame, before compression |
| `--compression` | `IBSEN_COMPRESSION` | `zstd` | codec for new frames: `zstd` or `none` |
| `--compressionLevel` | `IBSEN_COMPRESSION_LEVEL` | `default` | `fastest`, `default`, `better`, `best` |
| `-o, --readOnly` | `IBSEN_READ_ONLY` | `false` | serve a directory without changing a byte of it |
| `-e, --OTELExporter` | | | OpenTelemetry collector address, e.g. `0.0.0.0:4317` |
| `--certKey` / `--privateKey` | | | TLS for gRPC |
| `-v, --debug` / `-t, --trace` | | | log level |

An unknown `--compression` name is refused at startup rather than quietly falling back to
none, because falling back writes a log the operator did not ask for.

### The filter commands

`append`, `cat` and `topics` take `-d, --rootDirectory` (or `IBSEN_ROOT_DIRECTORY`) like the
server, and there is no in-memory fallback for them: a log that lives in one process and is
shared with nobody has nothing to append to. `append` also takes every write-side server flag
above — `--maxBlockSize`, `--flushEntries`, `--flushIntervalMs`, `--indexSparsity`,
`--maxFrameEntries`, `--maxFrameBytes`, `--compression`, `--compressionLevel` — because those
describe what is written, and a filter writes the same log a server does. The two flush flags
are the exception worth knowing about: they group concurrent writers, and a stream is one
writer, so `--batchSize` is what moves the throughput of an `append`. See [Durability](#durability).

| flag | commands | default | what it does |
|---|---|---|---|
| `--framing` | `append`, `cat` | `lines` | how the stream delimits entries: `lines`, or `length` for a little-endian uint64 byte count before each entry |
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

### Durability

A write is acknowledged, and its entries become readable, only once the flush covering them
has returned. A reader never sees an entry that a power cut could take back. There is no
background flusher goroutine: the writer that needs its entries durable drives the sync, and
writers whose entries joined the same batch wait on the same one.

The default is an fsync per write. The two flush flags work as a pair, and raising
`--flushEntries` on its own changes nothing: with `--flushIntervalMs 0` a batch is never held
back, so every write is flushed at once whatever the entry count says. Set an interval and the
count becomes the escape from it — "enough have arrived, do not wait out the rest" — and what
you are buying is writers whose entries wait together, at the cost of the latency of the ones
that arrive early. The entries are still never *readable* before they are durable. A store
that cannot sync, like `memstore`, pays none of this: its entries are durable the moment the
append returns.

For `append` none of that applies, because a stream is a single writer and there is nobody
else's batch for its entries to join: holding one back only makes it wait. **`--batchSize` is
the dial there.** One write is one append and one flush, so appending 126,574 lines costs 14
fsyncs at the default 10000 and 128 at `--batchSize 1000`, while `--flushEntries 10000` costs
the same as not passing it at all. A 400 MB stream of 12.6M lines took 11–22s at the default
against 55–86s at 1000, three runs each on an ordinary disk. Nothing about durability changes
with the batch size: every `append` that exits zero has its entries on durable media, and a
batch that fails takes with it only entries no one was told about.

**A failed fsync stops the topic.** The writes waiting on it get the error, and every later
write to that topic is refused until the process is restarted; other topics carry on. It is
not retried, because on Linux a retry succeeds without writing what the failed one dropped,
and the next write would be acknowledged behind a hole. On restart the head block is read from
the disk rather than the page cache (on Linux), so recovery sees the hole and cuts there. A failing fsync
usually means failing storage: look at the disk before you restart.

The size is a ceiling and not a quota. A batch is written when it is full, when it holds 16 MiB
of entries — the size counts entries, and entries have no size — **or when the stream has
nothing more ready**. So `tail -F applog | ibsen append` makes each line durable as it arrives
without being told to, and the same default still reads a file at one sync per 10000 lines: a
bulk load keeps the reader's buffer full, so its batches fill before they run dry. Appending
400 MB of 12.6M lines costs 1272 fsyncs against the 1264 the batch size alone asks for.

### Compression

**zstd by default.** A log is mostly text that repeats — the same field names, the same hosts,
the same shapes of message, entry after entry — so the bytes are worth compressing and the
choice is between disk and CPU rather than between features.

A setting only decides what is *written*. Every codec the binary links stays in the read
registry, so turning compression on, off or over never strands a block written under the old
setting; a frame carries one byte naming the codec that wrote it, which is also why one block
can hold frames of several codecs and why changing the setting rewrites nothing. Compression
that did not pay is thrown away per frame, so a frame a codec cannot shrink is stored plain
and a small write is never made larger.

What it costs and buys, appending 1.1 GB of wiki XML (21.7M lines) and reading it back, on an
ordinary ext4 disk with the page cache warm:

| | `--compression none` | `zstd` (default) |
|---|---|---|
| log on disk | 1.41 GiB | **456 MiB** |
| append, CPU | 10.7 s | 18.8 s |
| `cat > /dev/null` | 4.5–5.0 s | 6.2–6.9 s |

So it is a third of the disk for about three quarters more write CPU, and reads that are
slower when the bytes were already in memory and faster when they have to come off slower
storage than this. Two cases want `--compression none`: entries that are already compressed —
on incompressible payloads zstd measured 72 MB/s against 315 MB/s through the codec, for 10%
— and readers that seek rather than stream, since a frame is decoded whole and a single-entry
read at the default frame bound decodes 137 KB in 213 µs against 98 µs plain.

### Read-only mode

`--readOnly` builds a store that refuses every call that would change a file — not merely a
manager that turns writes away. This matters more than it looks: loading a topic recovers its
head block, truncates a torn tail and rebuilds its index. Pointed at a directory another
instance owns, a read-only server without a read-only store would quietly rewrite it.

`ibsen cat` and `ibsen topics` are always built this way, which is why they can be pointed at
a directory a server is writing without taking a lock or changing a byte.

---

## Operating it

### Docker

```shell
docker build -t ibsen .
docker run --name ibsen -p 54321:54321 ibsen
```

### TLS

```shell
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
ibsen server -d /tmp/ibsen/data --certKey cert.pem --privateKey key.pem
```

Paths are taken as given, relative to the working directory.

### OpenTelemetry

```shell
docker run -v ./collector-gateway.yaml:/etc/otelcol/config.yaml otel/opentelemetry-collector:0.54.0
ibsen server -d /tmp/ibsen/data -e 0.0.0.0:4317
```

### Profiling

```shell
ibsen server -d /tmp/ibsen/data -z cpu.pprof -y mem.pprof
go tool pprof cpu.pprof
```

### Benchmarking

```shell
ibsen server -d /tmp/ibsen/data
ibsen client bench mytopic --byteSize 100 --batchSize 1000 --bwb 1000 --brb 1000 --concurrent 4
```

To drop the page cache between runs on Linux: `echo 1 > /proc/sys/vm/drop_caches`.

### gRPC troubleshooting

```shell
GRPC_GO_LOG_VERBOSITY_LEVEL=99 GRPC_GO_LOG_SEVERITY_LEVEL=info ibsen client list
```

---

## The API

Three calls, defined in [`api/grpcApi/ibsen.proto`](api/grpcApi/ibsen.proto):

```proto
service Ibsen {
  rpc write (InputEntries) returns (WriteStatus);
  rpc read  (ReadParams)   returns (stream OutputEntries);
  rpc list  (EmptyArgs)    returns (TopicList);
}
```

`read` takes a topic, a starting offset, a batch size and `stopOnCompletion`: false keeps the
stream open and follows the log, true ends it when it reaches the end.

Those three calls are the whole of what Ibsen offers, and gRPC is one way to reach them.
Embedded programs get them as a Go interface, `driver.LogManager`; `ibsen append`, `ibsen cat`
and `ibsen topics` are the same three over stdin and stdout. Nothing in the log knows which
one is driving it.

Generate clients:

```shell
apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc --proto_path=api/grpcApi ibsen.proto --go_out=plugins=grpc:./
```

---

## Development

```shell
go test -race ./...               # the whole suite: about 105 s on two cores
go vet ./...
gofmt -l .
./scripts/check-architecture.sh   # the core is pure; dependencies point inward
./scripts/embedded-size.sh        # an embedded build is still small
```

CI runs all five on every push. The architecture check is the important one: it fails if any
package under `core/` reaches outside the standard library, if the core imports an adapter,
or if a driving adapter reaches a driven one.

### How it is tested

A log is worth exactly the promises it keeps when things go wrong — an acknowledged write is
there after a power cut, nothing is readable before it is durable, offsets never skip — so the
suite is built in levels, each holding what the ones below it cannot see:

| level | what it asks | how |
|---|---|---|
| **Static** | is the code shaped the way the architecture says | the dependency graph, the binary size, and an AST check that no log event is built and never sent |
| **Formats** | is every damaged byte caught | every byte of a frame header and of an index pair flipped and caught by its checksum; recovery against torn, corrupt, zeroed and old-format blocks |
| **Port conformance** | does every storage backend keep the contract | one suite run against the filesystem, memory and raw-flash stores |
| **Core properties** | does the log read back what was written | every offset, every store, four block sizes, live and reloaded, with and without its index |
| **Contracts** | do concurrent callers get what was promised | gated stores that hold a sync or a load still, so a test decides the interleaving instead of hoping for it |
| **Faults** | what survives a crash | torn writes below the store, and a power-cut model of the page cache: unsynced data lost, names not durable until their directory is synced, Linux's fsync-failure semantics |
| **Adapters and end to end** | does each piece do its job | the CLI, the Unix filter, the codec, the lease, the composition roots, and a real gRPC server on a free port |
| **System** | does one log explain everything every client saw | a Jepsen-style history checker over concurrent clients, a randomised nemesis cutting the power under them, and a Molly-style crash at every storage call of a deterministic workload, with every acknowledgement checked for durable support at the moment it is made |

Everything runs against real directories; nothing emulates a filesystem. The power-cut model
performs every call on a real directory and models only what a power cut would keep.

The system-level tests have found seven bugs in the log that the rest of the suite passed — a
topic whose name was never durable, a read that skipped entries, a failed fsync that was
retried, a rollover that could tear the block before it, a swallowed directory sync, and two
that only a restart after a failed fsync exposed. Each is fixed and pinned by its own test.

**[TESTING.md](TESTING.md)** is the developer's guide to all of it: the principles, every level,
the fault models, how to read a failure from the harnesses, recipes for fixing a bug or adding a
storage backend, and which test holds which promise.

### Design

The design, the file formats and the reasoning behind both are in
**[ARCHITECTURE.md](ARCHITECTURE.md)**. Project context, known gaps and the migration history
are in [CLAUDE.md](CLAUDE.md).

## Status

Under development, and usable: the core is covered by property tests, a conformance suite
every storage adapter passes, torn-write and power-cut fault injection, and history-checked
system tests that crash it at every storage call (see [How it is tested](#how-it-is-tested)).
There are no known correctness bugs. What is not done, and is known:

- Dictionaries for compression are designed, not built.
- Distribution beyond the file lease — the fencing token described above — is not built.
- Clients exist for Go; other languages are generated from the proto and nothing more.
- Nothing tests a client that retries a write, and writes carry no idempotency key, so a retry
  after an indeterminate failure can duplicate entries.

---

> *'One should not read to devour, but to see what can be applied.'*
> — Henrik Ibsen (1828–1906)

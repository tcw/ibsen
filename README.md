# Ibsen

<img src="mascot/ibsenMascot.svg" width="220" height="220" alt="Ibsen mascot" align="right">

**An append-only log that runs anywhere — SQLite for logs.**

Ibsen keeps logs: named streams of entries that you add to at the end and read back in order.
It is one small program you can run from the shell, a server you can talk to over the
network, or a Go library you link into your own program. Your data is plain files in a
directory you choose.

<br clear="right">

---

## What is a log?

Think of a notebook you only ever add lines to. You never rub a line out and you never squeeze
one in between two others. You just write the next line at the bottom.

Each line gets a number when it is written: the first is 0, the next is 1, and so on. That
number is called its **offset**. Once a line has an offset, the line never changes and the
number is never reused. So "line 42" means the same thing today, tomorrow and next year.

In Ibsen, one such notebook is called a **topic**, and one line in it is called an
**entry**. An entry can be any bytes, but it is often one line of text, such as a JSON event.

This is different from a database table. A table holds the current state of things: you
update a row and the old value is gone. A log holds what happened, in the order it happened,
and keeps all of it. If you want the current state, you can always rebuild it by reading the
log from the start.

It is also different from a message queue. In a queue, a message is taken off when someone
reads it, so only one reader sees it. In a log, reading changes nothing. Any number of
programs can read the same topic, each from wherever it likes, each at its own pace. A fast
reader can keep up with the newest entries while a slow one works through last week's. Each
reader only has to remember one number: the offset it got to.

Order and numbering are what make this useful. Every reader sees the entries in the same
order, so they all reach the same conclusions. And if a reader stops, it just picks up again
from the offset it remembered.

Logs are a good fit for:

- **Application events**, such as "user signed up" or "order shipped".
- **Audit trails**, where nothing may be changed after the fact.
- **Sensor and device data**, written as it arrives.
- **Feeding other programs**: one program writes, several others read and react.

## Why Ibsen — SQLite for logs

Most of the time you need the log, not a Kafka cluster. Kafka is a fine log, but it comes with
servers to run and keep healthy, long before your data is big enough to need them. SQLite
showed that a database can be a file and a library. Ibsen does the same for logs.

- **A library first.** The log is an ordinary value inside your Go program.
- **A command when that's all you need.** `ibsen append` and `ibsen cat` work on a directory
  with nothing else running.
- **A server when you want one.** The same log behind gRPC, in one binary.
- **Small enough to be boring.** The core uses only Go's standard library.
- **The file format is the interface.** Every entry carries its own checksum, so damage is
  caught, never served.

And it is safe: no reader ever sees an entry that a power cut could take back. Writes are
fast by default, and `--flushMode strict` makes every acknowledged write durable before Ibsen
says it is saved.

## Try it in a minute

### Install

Pick whichever you have.

- **A prebuilt binary** — needs nothing. Download the archive for your system from the
  [Releases page](https://github.com/tcw/ibsen/releases), unpack it, and put `ibsen` on
  your `PATH`.
- **Go** — needs Go 1.26 or later:

  ```shell
  go install github.com/tcw/ibsen@latest
  ```

- **Docker** — needs Docker. This starts a server on port 54321:

  ```shell
  docker run --rm -p 54321:54321 ghcr.io/tcw/ibsen:latest
  ```

Check it worked with `ibsen --version`.

### Your first log

Make a directory for the data:

```shell
mkdir -p /tmp/ibsen-data
```

Write two entries to a topic called `greetings`, one per line:

```shell
printf 'hello\nworld\n' | ibsen append -d /tmp/ibsen-data greetings
```

This prints nothing. The topic was created, and both entries are stored in
`/tmp/ibsen-data/greetings/`.

Read the topic back:

```shell
ibsen cat -d /tmp/ibsen-data greetings
```

```
hello
world
```

Read it again from offset 1, showing each entry's offset:

```shell
ibsen cat -d /tmp/ibsen-data greetings 1 --offsets
```

```
1	world
```

List the topics in the directory:

```shell
ibsen topics -d /tmp/ibsen-data
```

```
greetings
```

That is a working log. Add `--follow` to `cat` and it keeps printing new entries as they
arrive, like `tail -f`.

### The same with a server

Start a server. With no data directory it keeps everything in memory, which is handy for a
first look; add `-d /tmp/ibsen-data` to keep the data.

```shell
ibsen server
```

In another terminal, write and read over the network:

```shell
printf 'hello\nworld\n' | ibsen client write greetings
ibsen client read greetings
```

```
0	hello
1	world
```

`client read` keeps following the log until you press Ctrl-C.

Need a client in another language? `ibsen generate client java` (or `go`, `python`,
`typescript`) writes a folder that builds, with nothing else to install.

## Three ways to use it

**On the command line.** `ibsen append`, `ibsen cat` and `ibsen topics` read and write a data
directory directly, with no server. They work like any Unix filter, so you can pipe a topic
into `grep`, `jq` or another topic.

**As a server.** `ibsen server` offers the same log over gRPC, so programs on other machines,
in any language, can write and read it.

**Inside a Go program.** Import `wiring/embedded`, give it a place to store blocks, and the
log runs in your process with nothing else. See the
[embedded example](wiring/embedded/example/main.go) and the
[user guide](docs/USER_GUIDE.md#embed-it).

The same log core runs on a microcontroller with a chip of flash and in a container on
Kubernetes. A writer locks its data directory so a second one cannot write it; [choosing a
lock](docs/OPERATIONS.md#choosing-a-lock) says which lock fits where, including a Kubernetes
Lease for a StatefulSet.

## What Ibsen doesn't do

Ibsen has no built-in replication or consensus, by design. Keeping copies on several machines
is the job of the storage you run it on. If you need many writers and automatic failover in a
cluster, use Kafka. The reasoning is in
[ARCHITECTURE.md §16](ARCHITECTURE.md#16-single-writer-and-distribution).

## Status

Under development, and heavily tested: property tests, a storage conformance suite, power-cut
fault injection and history-checked crash tests. There are no known correctness bugs.
On one machine, the single-writer lock is held by the kernel where the filesystem supports it,
and a client can retry a write without storing it twice. Ready-to-build clients for Go, Java,
Python and TypeScript come bundled in the binary. What it deliberately leaves out is the section
above; what its tests do not yet cover is listed in [TESTING.md](TESTING.md#13-known-gaps).

## Learn more

- **[User guide](docs/USER_GUIDE.md)** — keeping data on disk, the filter commands, embedding
  the log in Go, and the gRPC API.
- **[Operations guide](docs/OPERATIONS.md)** — every flag, durability, compression, Docker,
  TLS, telemetry and the development commands.
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — how Ibsen is built and why: file formats, ports and
  the promises it keeps.
- **[TESTING.md](TESTING.md)** — how it is tested, and the bugs the tests have found.
- **[CLAUDE.md](CLAUDE.md)** — project context and the history of every change.

To work on Ibsen itself: `go test -race ./...`, and see
[Development](docs/OPERATIONS.md#development).

---

> *'One should not read to devour, but to see what can be applied.'*
> — Henrik Ibsen (1828–1906)

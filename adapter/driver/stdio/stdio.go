// Package stdio drives the log over a pair of byte streams, so a program can append to a
// topic and read one back with nothing between it and the bytes: no server, no socket, no
// daemon.
//
// It is a driving adapter like grpcapi, and it speaks the same port: everything here takes a
// driver.LogManager, which a composition root builds over whichever store it likes. That is
// why this package imports nothing but the core and the standard library, and why it cannot
// decide where the log lives — the program that wires it decides.
//
// Framing on the stream is a choice about the pipe, not about the format on disk. Lines is
// the default because it composes with the tools a stream is usually handed to; Length
// carries bytes a line cannot. A log written through one is readable through either.
package stdio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/errore"
)

// Framing is how entries are delimited on a stream. It says nothing about the log: the
// entries are the same bytes either way.
type Framing uint8

const (
	// Lines delimits entries with a newline, which is what makes a topic something grep, jq
	// and wc can be handed. An entry holding a newline cannot survive it, which is what
	// Length is for. An empty line is an empty entry rather than nothing: dropping it would
	// mean the stream that comes back out is not the one that went in.
	Lines Framing = iota
	// Length prefixes each entry with its byte count as a little-endian uint64, the same
	// shape the entry codec uses on disk. Any byte string survives it, including one holding
	// newlines or nothing at all.
	Length
)

const (
	// DefaultAppendBatchSize is how many entries one Write carries when the caller does not
	// choose. One Write is one append and, on a store that syncs, one fsync, so this is what
	// a stream costs: 10000 was where the gain flattened when it was measured, and 100000
	// bought nothing more. It is larger than the read default because the two are not the
	// same trade: a large write saves syncs, while a large read only holds more entries in
	// memory before the first of them reaches the stream.
	DefaultAppendBatchSize = 10000
	// DefaultCatBatchSize is how many entries a read asks for at a time.
	DefaultCatBatchSize = 1000
	// MaxBatchBytes bounds the entry bytes held for one Write. The batch size counts
	// entries, and entries have no size: without this, a stream of large ones would hold the
	// batch size times the largest entry in memory before writing any of it. One entry
	// larger than this still gets a write, of its own.
	MaxBatchBytes = 16 << 20
	// DefaultPollInterval is how long Cat waits before looking again while following.
	DefaultPollInterval = 100 * time.Millisecond
	// MaxLineSize bounds one newline-delimited entry, since a stream with no newline in it
	// would otherwise be read into memory whole.
	MaxLineSize = 10 << 20
)

// ErrOffsetsNeedLines is returned for offsets asked of a framing that has nowhere to put
// them. Length framing carries entries and nothing else, on purpose: a reader of it knows
// how to find the next entry without being told anything.
var ErrOffsetsNeedLines = errors.New("stdio: offsets can only be printed with line framing")

// ErrTruncatedEntry is returned by Append when a length-framed stream ends inside an entry,
// which means the stream was cut rather than finished.
var ErrTruncatedEntry = errors.New("stdio: stream ended inside an entry")

// ErrLineTooLong is a newline-delimited entry larger than MaxLineSize. A stream with no
// newline in it would otherwise be read into memory whole; length framing carries entries
// that large, and says how large before they are read.
var ErrLineTooLong = errors.New("stdio: line longer than MaxLineSize")

// AppendParams is how a stream is read into the log.
type AppendParams struct {
	// Framing is how the incoming stream delimits entries.
	Framing Framing
	// BatchSize is how many entries go in one Write; zero means DefaultAppendBatchSize. It
	// is a ceiling rather than a quota: a batch is written when it is full, when it holds
	// MaxBatchBytes, when the stream has nothing more ready, or when the stream ends.
	//
	// It is a batching choice and not a durability one: a Write returns when the entries it
	// carried are durable, whatever the size. What it decides is the cost of that — one
	// Write is one append and, on a store that syncs, one flush — so it is the throughput
	// dial for a stream, and the only one. The flush policy is not: a stream is a single
	// writer, so there is never anybody else's batch for its entries to join, and holding a
	// batch back for company that cannot come only makes it wait.
	BatchSize int
}

// Append reads entries from in and writes them to the topic, returning how many it wrote.
// It returns when the stream ends.
//
// A write that fails stops the append, and the entries already written stay written: there
// is no transaction across batches, and there is nothing to roll back to, because everything
// acknowledged is on durable media already.
func Append(log driver.LogManager, topic domain.TopicName, in io.Reader, params AppendParams) (uint64, error) {
	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultAppendBatchSize
	}
	reader, err := newEntryReader(in, params.Framing)
	if err != nil {
		return 0, err
	}
	var written uint64
	// the batch size comes from a flag, so the room made for it up front is capped; a larger
	// one is still honoured, it just grows into it
	prealloc := min(batchSize, DefaultAppendBatchSize)
	batch := make([][]byte, 0, prealloc)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := log.Write(topic, &batch); err != nil {
			return err
		}
		written += uint64(len(batch))
		batch = make([][]byte, 0, prealloc)
		batchBytes = 0
		return nil
	}
	for {
		entry, err := reader.next()
		if errors.Is(err, io.EOF) {
			return written, flush()
		}
		if err != nil {
			// the entries read before the cut are whole entries, so they are written: what
			// survives a damaged stream must not depend on where the batch boundary happened
			// to fall, which is a tuning choice and nothing to do with the stream
			if flushErr := flush(); flushErr != nil {
				return written, errore.WrapError(flushErr, err)
			}
			return written, err
		}
		batch = append(batch, entry)
		batchBytes = batchBytes + len(entry)
		// a full batch is written, and so is one with nothing behind it: a stream that has
		// gone quiet leaves what it has already said on durable media rather than in memory
		if len(batch) == batchSize || batchBytes >= MaxBatchBytes || !reader.buffered() {
			if err := flush(); err != nil {
				return written, err
			}
		}
	}
}

// entryReader hands back one entry of a stream at a time, and says whether another one is
// already there. Both framings read through one bufio.Reader, which is what makes the second
// question answerable: a bufio.Scanner keeps a buffer of its own and will not say what is
// behind it, so an appender built on one can only block on the next entry or stop.
type entryReader struct {
	in      *bufio.Reader
	framing Framing
	header  [8]byte
}

// readerBufferSize is how much of the stream is held ahead of the appender. It is large
// because the buffer running empty is how the appender decides to write what it has (see
// buffered), and a small one would make a bulk load flush every time it ran dry rather than
// when its batch was full. This way a stream of ordinary lines fills a batch first, and a
// sync costs at most this much of the stream however small the entries are.
const readerBufferSize = 1 << 20

func newEntryReader(in io.Reader, framing Framing) (*entryReader, error) {
	if framing != Lines && framing != Length {
		return nil, fmt.Errorf("stdio: unknown framing %d", framing)
	}
	return &entryReader{in: bufio.NewReaderSize(in, readerBufferSize), framing: framing}, nil
}

// buffered reports whether the next entry can be read without waiting on the stream. It is
// what lets a batch be written rather than held: a feed that has gone quiet leaves the
// buffer empty, and the entries already read are written instead of waiting for company that
// is not coming. A stream arriving faster than it is written keeps the buffer full, so its
// batches fill and the check costs nothing.
//
// It is a hint and not a promise: the bytes left may be half an entry, and the read after
// them blocks. Nothing but batching depends on the answer.
func (r *entryReader) buffered() bool {
	return r.in.Buffered() > 0
}

// next reads one entry, returning io.EOF when the stream has ended cleanly. The entry is the
// caller's: nothing here holds a reference to it.
func (r *entryReader) next() ([]byte, error) {
	if r.framing == Length {
		return r.nextLengthFramed()
	}
	return r.nextLine()
}

// nextLine reads up to the next newline, or to the end of a stream that does not end with
// one. The line is copied out of the reader's buffer, which the next read refills, and a
// line longer than that buffer is collected across several of them.
func (r *entryReader) nextLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.in.ReadSlice('\n')
		switch err {
		case nil:
			chunk = chunk[:len(chunk)-1]
		case bufio.ErrBufferFull, io.EOF:
		default:
			return nil, err
		}
		if err == io.EOF && len(line) == 0 && len(chunk) == 0 {
			// the stream ended on an entry boundary, which is the end of it
			return nil, io.EOF
		}
		if len(line)+len(chunk) > MaxLineSize {
			return nil, ErrLineTooLong
		}
		line = append(line, chunk...)
		if err == bufio.ErrBufferFull {
			// the line is longer than the buffer, so there is more of it to come
			continue
		}
		if line == nil {
			// a blank line is an empty entry, not nothing
			line = []byte{}
		}
		return line, nil
	}
}

// nextLengthFramed reads one entry behind its little-endian uint64 byte count.
func (r *entryReader) nextLengthFramed() ([]byte, error) {
	if _, err := io.ReadFull(r.in, r.header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			// a stream that ends between entries has ended cleanly
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTruncatedEntry
		}
		return nil, err
	}
	size := binary.LittleEndian.Uint64(r.header[:])
	if size > uint64(domain.MaxEntrySize) {
		return nil, fmt.Errorf("stdio: entry of %d bytes is larger than the %d a log entry can hold",
			size, domain.MaxEntrySize)
	}
	entry := make([]byte, size)
	if _, err := io.ReadFull(r.in, entry); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTruncatedEntry
		}
		return nil, err
	}
	return entry, nil
}

// CatParams is how a topic is written to a stream.
type CatParams struct {
	// From is the offset to start at.
	From domain.Offset
	// BatchSize is how many entries are asked for at a time; zero means DefaultCatBatchSize.
	BatchSize uint32
	// Framing is how entries are delimited on the way out.
	Framing Framing
	// Offsets prefixes each entry with its offset and a tab. Lines framing only, and off by
	// default, so that what comes out of a topic is what went in and can go straight into
	// another one.
	Offsets bool
	// Follow keeps reading as entries arrive instead of stopping at the end of the log.
	Follow bool
	// PollInterval is how long to wait before looking again while following; zero means
	// DefaultPollInterval.
	PollInterval time.Duration
	// Cancel stops the read when closed; nil never cancels.
	Cancel <-chan struct{}
}

// Cat writes the topic's entries to out and returns the offset after the last one written,
// which is where a later call carries on from. Without Follow it returns at the end of the
// log; with it, it returns when Cancel is closed or the stream cannot be written to.
//
// A stream that cannot be written to is the normal way this ends: `ibsen cat topic | head -5`
// closes the pipe, and the write that fails cancels the read rather than leaving it filling
// a buffer nobody will drain.
//
// Follow keeps asking this log manager for more, which sees everything written through it.
// It does not see what another process appends to the same directory, because a loaded topic
// holds the block list it loaded; a driver that has to follow across processes opens the log
// again, from the offset this returns.
func Cat(log driver.LogManager, topic domain.TopicName, out io.Writer, params CatParams) (domain.Offset, error) {
	if params.Offsets && params.Framing != Lines {
		return params.From, ErrOffsetsNeedLines
	}
	if params.BatchSize == 0 {
		params.BatchSize = DefaultCatBatchSize
	}
	poll := params.PollInterval
	if poll == 0 {
		poll = DefaultPollInterval
	}
	writer := bufio.NewWriter(out)
	from := params.From
	for {
		sentUntil, readErr, writeErr := catOnce(log, topic, writer, from, params)
		from = sentUntil
		if writeErr != nil {
			return from, writeErr
		}
		if err := writer.Flush(); err != nil {
			return from, err
		}
		switch {
		case errors.Is(readErr, domain.ErrReadCancelled):
			return from, nil
		case errors.Is(readErr, domain.NoEntriesFound):
			// nothing at this offset yet, which at the end of a log is not a failure
		case readErr != nil:
			return from, readErr
		}
		if !params.Follow {
			return from, nil
		}
		select {
		case <-params.Cancel:
			return from, nil
		case <-time.After(poll):
		}
	}
}

// catOnce writes one pass over the log, from offset to the end as it stands. It returns the
// offset after the last entry written, the read's error and the stream's.
//
// Every batch the read hands over is acknowledged even after the stream has failed, because
// the read blocks until it is: dropping out of the loop early would leave it waiting forever
// on a consumer that has gone.
func catOnce(log driver.LogManager, topic domain.TopicName, out *bufio.Writer, from domain.Offset, params CatParams) (domain.Offset, error, error) {
	cancel := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(cancel) }) }
	defer stop()
	if params.Cancel != nil {
		go func() {
			select {
			case <-params.Cancel:
				stop()
			case <-cancel:
			}
		}()
	}

	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	readDone := make(chan error, 1)
	go func() {
		readDone <- log.Read(driver.ReadParams{
			TopicName: topic,
			From:      from,
			BatchSize: params.BatchSize,
			LogChan:   logChan,
			Wg:        &wg,
			Cancel:    cancel,
		})
		close(logChan)
	}()

	next := from
	var writeErr error
	for batch := range logChan {
		if writeErr == nil && len(*batch) > 0 {
			writeErr = writeBatch(out, batch, params)
			if writeErr != nil {
				stop()
			} else {
				next = domain.Offset((*batch)[len(*batch)-1].Offset + 1)
			}
		}
		wg.Done()
	}
	return next, <-readDone, writeErr
}

// writeBatch puts one batch on the stream and flushes it, so a follower sees entries as they
// arrive and a closed pipe is noticed now rather than at the end.
func writeBatch(out *bufio.Writer, batch *[]domain.LogEntry, params CatParams) error {
	for _, entry := range *batch {
		if params.Framing == Length {
			var header [8]byte
			binary.LittleEndian.PutUint64(header[:], uint64(len(entry.Entry)))
			if _, err := out.Write(header[:]); err != nil {
				return err
			}
			if _, err := out.Write(entry.Entry); err != nil {
				return err
			}
			continue
		}
		if params.Offsets {
			if _, err := fmt.Fprintf(out, "%d\t", entry.Offset); err != nil {
				return err
			}
		}
		if _, err := out.Write(entry.Entry); err != nil {
			return err
		}
		if err := out.WriteByte('\n'); err != nil {
			return err
		}
	}
	return out.Flush()
}

// List writes the topic names, one per line. It is the third verb the driving port offers,
// and the only one that needs no framing choice: a topic name holds neither a newline nor a
// control character, because domain.ValidateTopicName refuses one that does.
func List(log driver.LogManager, out io.Writer) error {
	writer := bufio.NewWriter(out)
	for _, name := range log.List() {
		if _, err := fmt.Fprintln(writer, string(name)); err != nil {
			return err
		}
	}
	return writer.Flush()
}

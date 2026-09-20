package cli

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/wiring"
)

// The commands here open a data directory directly: no server, no socket, no daemon. They
// are the stdio driving adapter with a composition root under it, which is why they take the
// data directory rather than a host and a port.
//
// Reading takes no lock and is safe against a directory somebody else is writing, because it
// opens a read-only store. Appending takes the same single-writer lease the server takes, and
// is therefore refused while a server holds the directory. That is the lock doing its job.
var (
	framing string
	offsets bool
	follow  bool
	// append and cat have a batch size each, and not one between them: cobra writes a flag's
	// default into its variable as the flag is registered, so a shared one would end up
	// holding whichever command was registered last.
	appendBatchSize int
	catBatchSize    int
	pollMs          int

	cmdAppend = &cobra.Command{
		Use:              "append [topic]",
		Short:            "append entries from stdin to a topic, without a server",
		Long:             `Reads entries from stdin and appends them to a topic in a data directory, taking the single writer lock for as long as it runs.`,
		TraverseChildren: true,
		Args:             cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			setLogLevel(zerolog.WarnLevel)
			if err := runAppend(domain.TopicName(args[0])); err != nil {
				fail(err, "unable to append to topic %s", args[0])
			}
		},
	}

	cmdCat = &cobra.Command{
		Use:              "cat [topic] [offset (default=0)]",
		Short:            "write a topic to stdout, without a server",
		Long:             `Writes a topic's entries to stdout from a data directory. Opens the log read-only, so it is safe to point at a directory a server is writing.`,
		TraverseChildren: true,
		Args:             cobra.RangeArgs(1, 2),
		Run: func(cmd *cobra.Command, args []string) {
			setLogLevel(zerolog.WarnLevel)
			from, err := parseOffsetArg(args)
			if err != nil {
				fail(err, "unable to read topic %s", args[0])
			}
			if err := runCat(domain.TopicName(args[0]), from); err != nil {
				fail(err, "unable to read topic %s", args[0])
			}
		},
	}

	cmdTopics = &cobra.Command{
		Use:              "topics",
		Short:            "list the topics in a data directory, without a server",
		Long:             `Writes the names of the topics in a data directory to stdout, one per line.`,
		TraverseChildren: true,
		Args:             cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			setLogLevel(zerolog.WarnLevel)
			if err := runTopics(); err != nil {
				fail(err, "unable to list topics")
			}
		},
	}
)

// runAppend writes stdin into the topic. The log is closed before anything is reported, so
// the write, its flush and the indexing it started are finished and the lease is released
// even when the append fails: log.Fatal exits without running deferred calls, which is why
// nothing here fatals while the log is open.
func runAppend(topic domain.TopicName) error {
	chosen, err := parseFraming(framing)
	if err != nil {
		return err
	}
	if err := validateFrameBounds(maxFrameEntries, maxFrameBytes); err != nil {
		return err
	}
	root, err := localRoot()
	if err != nil {
		return err
	}
	local, err := wiring.OpenLocal(wiring.LocalParams{
		RootPath:         root,
		MaxBlockSize:     maxBlockSizeMB * 1024 * 1024,
		IndexSparsity:    uint32(indexSparsity),
		MaxFrameEntries:  uint32(maxFrameEntries),
		MaxFrameBytes:    maxFrameBytes,
		FlushEntries:     uint32(flushEntries),
		FlushInterval:    time.Duration(flushIntervalMs) * time.Millisecond,
		Compression:      compression,
		CompressionLevel: compressionLevel,
	})
	if err != nil {
		return localOpenError(root, err)
	}
	defer local.Close()
	written, err := stdio.Append(local, topic, os.Stdin, stdio.AppendParams{
		Framing:   chosen,
		BatchSize: appendBatchSize,
	})
	if err != nil {
		return errore.WrapWithContextF(err, "wrote %d entries before failing", written)
	}
	log.Debug().Msgf("appended %d entries to %s", written, topic)
	return nil
}

// runCat writes the topic to stdout, once, or until interrupted when following.
//
// Following opens the log again for every pass rather than holding one open and asking it for
// more. It has to: a loaded topic holds the block list it loaded and learns about new blocks
// and new offsets only from writes made through it, so a log held open would follow a
// directory another process is appending to exactly as far as it had got when it opened —
// silently, which is the worst way for a tail to be wrong. Opening again is what a process
// that does not own the log can do without changing the core, and it is the caching described
// in CLAUDE.md §11 rather than a limitation of the adapter.
//
// What it costs is a load per pass, and a load recovers the head block by scanning it, so a
// large head block is worth a longer --pollMs. Nothing is written by any of it: the store is
// read-only.
func runCat(topic domain.TopicName, from domain.Offset) error {
	chosen, err := parseFraming(framing)
	if err != nil {
		return err
	}
	root, err := localRoot()
	if err != nil {
		return err
	}
	params := stdio.CatParams{
		From:      from,
		BatchSize: uint32(catBatchSize),
		Framing:   chosen,
		Offsets:   offsets,
	}
	if !follow {
		_, err := catPass(root, topic, params)
		return err
	}
	cancel := cancelOnSignal()
	for {
		next, err := catPass(root, topic, params)
		if err != nil {
			return err
		}
		params.From = next
		select {
		case <-cancel:
			return nil
		case <-time.After(time.Duration(pollMs) * time.Millisecond):
		}
	}
}

// catPass opens the data directory read-only, writes what the topic holds from the given
// offset, and closes it again. The topic is looked up rather than created: a read-only store
// refuses the directory a load would otherwise make, and "the filesystem is read only" is a
// poor way to say a topic does not exist.
func catPass(root string, topic domain.TopicName, params stdio.CatParams) (domain.Offset, error) {
	local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: root, ReadOnly: true})
	if err != nil {
		return params.From, localOpenError(root, err)
	}
	defer local.Close()
	if !holdsTopic(local.List(), topic) {
		return params.From, userErrorf("topic %s not found in %s", topic, root)
	}
	return stdio.Cat(local, topic, os.Stdout, params)
}

func runTopics() error {
	root, err := localRoot()
	if err != nil {
		return err
	}
	local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: root, ReadOnly: true})
	if err != nil {
		return localOpenError(root, err)
	}
	defer local.Close()
	return stdio.List(local, os.Stdout)
}

func holdsTopic(topics []domain.TopicName, wanted domain.TopicName) bool {
	for _, name := range topics {
		if name == wanted {
			return true
		}
	}
	return false
}

// localRoot is the data directory these commands work on. There is no in-memory fallback the
// way the server has one: a log that lives in the process and is shared with nobody would
// have nothing to append to and nothing to read back.
func localRoot() (string, error) {
	if rootDirectory == "" {
		return "", userErrorf("a data directory is required, give one with --rootDirectory or IBSEN_ROOT_DIRECTORY")
	}
	return AbsOrEmpty(rootDirectory), nil
}

// parseFraming turns the flag into the framing the stream is read or written with.
func parseFraming(name string) (stdio.Framing, error) {
	switch name {
	case "", "lines":
		return stdio.Lines, nil
	case "length":
		return stdio.Length, nil
	default:
		return 0, userErrorf("framing %q is not one of lines, length", name)
	}
}

// parseOffsetArg reads the optional offset of "cat <topic> [offset]". An offset that is not a
// number is an error rather than a zero, for the reason parseReadArgs gives.
func parseOffsetArg(args []string) (domain.Offset, error) {
	if len(args) < 2 {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return 0, userErrorf("offset %q is not a number", args[1])
	}
	return domain.Offset(parsed), nil
}

// cancelOnSignal closes the returned channel on interrupt, so a follow stops and the log is
// closed on the way out instead of the process being cut off mid-batch.
func cancelOnSignal() <-chan struct{} {
	cancel := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(cancel)
	}()
	return cancel
}

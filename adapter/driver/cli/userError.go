package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/wiring"
)

// A command fails in two ways, and they want to be told apart.
//
// One is a mistake the person running it can correct: a topic that is not there, a data
// directory that is not there, a flag value that is not allowed, another process holding the
// write lock. Nothing has gone wrong with the log, so the answer is one line on stderr and a
// non-zero exit. An FTL with a timestamp and "at github.com/tcw/ibsen/...(localCmd.go:184)"
// in front of it says the opposite — it reads as Ibsen having broken, and a stack trace is
// not an answer to "is there a topic called greetings".
//
// The other is a failure: a torn block, a refused write, a broken pipe. That still gets the
// error with everything errore collected on the way up, because that one is a diagnostic and
// wants to look like one.

// userError is the first kind. It carries a message and nothing else, since it is printed
// rather than investigated.
type userError struct{ msg string }

func (e userError) Error() string { return e.msg }

// userErrorf builds a message about the command rather than about the log. It is deliberately
// not errore.NewF: the caller of a command has no use for the line that refused it.
func userErrorf(format string, v ...any) error {
	return userError{msg: fmt.Sprintf(format, v...)}
}

// userMessage returns the line to print for an error the person running the command can act
// on, and whether it is one. Errors the stdio adapter raises about the command rather than
// about the log are named here, since a driven-side package has no CLI to defer to.
func userMessage(err error) (string, bool) {
	var user userError
	if errors.As(err, &user) {
		return user.msg, true
	}
	if errors.Is(err, stdio.ErrOffsetsNeedLines) {
		return "--offsets needs line framing, since length framing carries entries and nothing else", true
	}
	return "", false
}

// localOpenError turns what OpenLocal refuses into a message about the command that asked.
// Both conditions are answers rather than faults: the directory is not there, or somebody
// else is writing it, and in neither case has anything gone wrong.
func localOpenError(root string, err error) error {
	switch {
	case errors.Is(err, wiring.ErrRootNotFound):
		return userErrorf("%s is not there, give a data directory that exists", root)
	case errors.Is(err, wiring.ErrWriteLockUnavailable):
		return userErrorf("%s is being written by another process, and a log takes one writer at a time", root)
	case errors.Is(err, wiring.ErrUnknownCompression):
		return userErrorf("compression %q is not one of none, zstd", compression)
	default:
		return err
	}
}

// fail reports a command that could not do its work and exits non-zero, either way. Which
// way is the distinction above: a message, or the diagnostic with its trace.
func fail(err error, format string, v ...any) {
	if msg, ok := userMessage(err); ok {
		fmt.Fprintf(os.Stderr, "ibsen: %s\n", msg)
		os.Exit(1)
	}
	log.Fatal().Err(err).Msgf(format, v...)
}

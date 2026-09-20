package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/wiring"
)

// TestUserMessageTellsAnAnswerFromAFailure: the distinction fail rests on. A mistake the
// person running the command can correct is printed; anything else keeps the trace errore
// collected, because that one is a diagnostic.
func TestUserMessageTellsAnAnswerFromAFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "a message about the command", err: userErrorf("topic %s not found in %s", "greetings", "/data"),
			want: "topic greetings not found in /data"},
		{name: "one wrapped on its way up", err: fmt.Errorf("opening the log: %w", userErrorf("offset %q is not a number", "tail")),
			want: `offset "tail" is not a number`},
		{name: "offsets asked of length framing", err: fmt.Errorf("cat: %w", stdio.ErrOffsetsNeedLines),
			want: "--offsets needs line framing, since length framing carries entries and nothing else"},
	} {
		got, ok := userMessage(test.err)
		if !ok {
			t.Errorf("%s: reported as a failure, want a message", test.name)
			continue
		}
		if got != test.want {
			t.Errorf("%s: message %q, want %q", test.name, got, test.want)
		}
	}
	if msg, ok := userMessage(errore.New("the head block is torn")); ok {
		t.Errorf("a failure was reported as the message %q, want the diagnostic with its trace", msg)
	}
}

// TestAUserMessageCarriesNoStackTrace is the bug itself: the line printed for a topic that is
// not there used to be "at github.com/tcw/ibsen/adapter/driver/cli.catPass(.../localCmd.go:184)
// topic greetings not found in /data", which reads as Ibsen having failed rather than as an
// answer about the log.
func TestAUserMessageCarriesNoStackTrace(t *testing.T) {
	for _, err := range []error{
		userErrorf("topic %s not found in %s", "greetings", "/data"),
		localOpenError("/data", fmt.Errorf("opening: %w", wiring.ErrRootNotFound)),
		localOpenError("/data", fmt.Errorf("opening: %w", wiring.ErrWriteLockUnavailable)),
		func() error { _, err := parseFraming("netstring"); return err }(),
		func() error { _, err := parseOffsetArg([]string{"topic", "tail"}); return err }(),
		validateFrameBounds(0, 1),
	} {
		msg, ok := userMessage(err)
		if !ok {
			t.Errorf("%v is not a message the person running the command can act on", err)
			continue
		}
		if strings.Contains(msg, ".go:") || strings.Contains(msg, "github.com/tcw/ibsen") {
			t.Errorf("message names a source line, want only what the caller can act on: %q", msg)
		}
	}
}

// TestLocalOpenErrorLeavesAFailureAlone: only the conditions OpenLocal names as answers are
// translated. A store that could not be opened for any other reason is still a diagnostic.
func TestLocalOpenErrorLeavesAFailureAlone(t *testing.T) {
	failure := errore.New("permission denied")
	if got := localOpenError("/data", failure); got != failure {
		t.Errorf("localOpenError rewrote a failure into %v, want it passed through", got)
	}
}

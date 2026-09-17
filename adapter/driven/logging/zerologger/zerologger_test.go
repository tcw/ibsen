package zerologger

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/tcw/ibsen/core/port/driven"
)

func newTestLogger(t *testing.T, level zerolog.Level) (Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	return New(zerolog.New(buf).Level(level)), buf
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var event map[string]any
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("event was not JSON: %v (%q)", err, buf.String())
	}
	return event
}

func TestEveryFieldKindReachesTheBackend(t *testing.T) {
	log, buf := newTestLogger(t, zerolog.TraceLevel)

	log.Log(driven.LevelWarn, "truncated torn tail of log block",
		driven.Str("topic", "t"),
		driven.Int("logBlocks", -3),
		driven.Int64("truncatedBytes", 1<<40),
		driven.Uint64("logBlock", 1<<63),
		driven.Bool("index_updated", true),
		driven.Err(errors.New("boom")))

	event := decode(t, buf)
	for key, want := range map[string]any{
		"level":          "warn",
		"message":        "truncated torn tail of log block",
		"topic":          "t",
		"logBlocks":      float64(-3),
		"truncatedBytes": float64(1 << 40),
		"logBlock":       float64(1 << 63),
		"index_updated":  true,
		"error":          "boom",
	} {
		if got := event[key]; got != want {
			t.Errorf("field %q = %v (%T), want %v", key, got, got, want)
		}
	}
}

func TestEveryLevelMaps(t *testing.T) {
	for _, tc := range []struct {
		level driven.Level
		want  string
	}{
		{driven.LevelTrace, "trace"},
		{driven.LevelDebug, "debug"},
		{driven.LevelInfo, "info"},
		{driven.LevelWarn, "warn"},
		{driven.LevelError, "error"},
	} {
		log, buf := newTestLogger(t, zerolog.TraceLevel)
		log.Log(tc.level, "msg")
		if got := decode(t, buf)["level"]; got != tc.want {
			t.Errorf("%v logged at level %v, want %v", tc.level, got, tc.want)
		}
		if tc.level.String() != tc.want {
			t.Errorf("Level(%d).String() = %q, want %q", tc.level, tc.level.String(), tc.want)
		}
	}
}

// Enabled is what the core asks before building a debug payload, so it has to agree with
// what Log actually emits at that level.
func TestEnabledAgreesWithWhatIsEmitted(t *testing.T) {
	for _, level := range []driven.Level{
		driven.LevelTrace, driven.LevelDebug, driven.LevelInfo, driven.LevelWarn, driven.LevelError,
	} {
		log, buf := newTestLogger(t, zerolog.WarnLevel)
		enabled := log.Enabled(level)
		log.Log(level, "msg")
		emitted := buf.Len() > 0
		if enabled != emitted {
			t.Errorf("level %v: Enabled reported %t but emitting produced %t", level, enabled, emitted)
		}
		if want := level >= driven.LevelWarn; enabled != want {
			t.Errorf("level %v under a warn threshold: Enabled = %t, want %t", level, enabled, want)
		}
	}
}

func TestNilErrorIsDropped(t *testing.T) {
	log, buf := newTestLogger(t, zerolog.TraceLevel)
	log.Log(driven.LevelError, "failed listing topics", driven.Err(nil))
	if _, present := decode(t, buf)["error"]; present {
		t.Errorf("a nil error should not be logged, got %q", buf.String())
	}
}

func TestNopLoggerDiscardsAndReportsDisabled(t *testing.T) {
	var log driven.Logger = driven.NopLogger{}
	for _, level := range []driven.Level{driven.LevelTrace, driven.LevelError} {
		if log.Enabled(level) {
			t.Errorf("NopLogger reported level %v enabled", level)
		}
	}
	log.Log(driven.LevelError, "msg", driven.Err(errors.New("boom")))
}

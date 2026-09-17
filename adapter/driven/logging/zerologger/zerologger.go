// Package zerologger routes the core's logging port to zerolog.
package zerologger

import (
	"github.com/rs/zerolog"

	"github.com/tcw/ibsen/core/port/driven"
)

// Logger adapts a zerolog.Logger to driven.Logger.
type Logger struct {
	log zerolog.Logger
}

var _ driven.Logger = Logger{}

// New adapts an existing zerolog.Logger, so the process keeps one logging configuration.
func New(log zerolog.Logger) Logger {
	return Logger{log: log}
}

func (l Logger) Enabled(level driven.Level) bool {
	return zlevel(level) >= l.log.GetLevel()
}

func (l Logger) Log(level driven.Level, msg string, fields ...driven.Field) {
	event := l.log.WithLevel(zlevel(level))
	if !event.Enabled() {
		return
	}
	for _, f := range fields {
		switch f.Kind {
		case driven.KindString:
			event = event.Str(f.Key, f.Str)
		case driven.KindInt:
			event = event.Int64(f.Key, f.Int)
		case driven.KindUint:
			event = event.Uint64(f.Key, f.Uint)
		case driven.KindBool:
			event = event.Bool(f.Key, f.Bool)
		case driven.KindError:
			if f.Err != nil {
				event = event.Err(f.Err)
			}
		}
	}
	event.Msg(msg)
}

func zlevel(level driven.Level) zerolog.Level {
	switch level {
	case driven.LevelTrace:
		return zerolog.TraceLevel
	case driven.LevelDebug:
		return zerolog.DebugLevel
	case driven.LevelInfo:
		return zerolog.InfoLevel
	case driven.LevelWarn:
		return zerolog.WarnLevel
	case driven.LevelError:
		return zerolog.ErrorLevel
	default:
		return zerolog.NoLevel
	}
}

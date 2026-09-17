package driven

// Logger is the logging port. The core has things worth saying — a torn tail it truncated,
// a stray file it ignored, an index update that failed in the background — and no opinion
// about where they go. An adapter routes them to zerolog, to a UART, or nowhere.
//
// It is two methods on purpose. An adapter has to route a level, a message and a few typed
// fields, nothing more: there is no event builder to implement, no formatting verbs, no
// hierarchy of child loggers. Enabled is here because the core builds a payload for some
// debug events, and should not pay for one that will be discarded.
//
// Implementations must be safe for concurrent use.
type Logger interface {
	// Enabled reports whether an event at this level would be emitted.
	Enabled(level Level) bool

	// Log emits one event. Fields with a nil Err are dropped.
	Log(level Level, msg string, fields ...Field)
}

// Level orders events from noise to failure. An adapter maps these onto whatever its
// backend calls them.
type Level uint8

const (
	LevelTrace Level = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelTrace:
		return "trace"
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// FieldKind says which of a Field's values carries its payload.
type FieldKind uint8

const (
	KindString FieldKind = iota
	KindInt
	KindUint
	KindBool
	KindError
)

// Field is one key and one typed value. The types are the ones the core actually logs, so
// an adapter can switch on Kind exhaustively and never reach for reflection.
type Field struct {
	Key  string
	Kind FieldKind
	Str  string
	Int  int64
	Uint uint64
	Bool bool
	Err  error
}

func Str(key, value string) Field {
	return Field{Key: key, Kind: KindString, Str: value}
}

func Int(key string, value int) Field {
	return Field{Key: key, Kind: KindInt, Int: int64(value)}
}

func Int64(key string, value int64) Field {
	return Field{Key: key, Kind: KindInt, Int: value}
}

func Uint64(key string, value uint64) Field {
	return Field{Key: key, Kind: KindUint, Uint: value}
}

func Bool(key string, value bool) Field {
	return Field{Key: key, Kind: KindBool, Bool: value}
}

// Err carries the cause of an event. Its key is fixed so adapters can render it the way
// their backend renders errors rather than as one more string field.
func Err(err error) Field {
	return Field{Key: "error", Kind: KindError, Err: err}
}

// NopLogger discards everything and reports every level disabled, so a core built without a
// logging adapter neither crashes nor builds payloads. It is the default, which is what lets
// an embedded build carry no logging code at all.
type NopLogger struct{}

var _ Logger = NopLogger{}

func (NopLogger) Enabled(Level) bool          { return false }
func (NopLogger) Log(Level, string, ...Field) {}

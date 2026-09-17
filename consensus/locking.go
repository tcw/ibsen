package consensus

// SingleIbsenWriterLock is the coordination port: the core needs to know it is the only
// writer, and does not care whether that is guaranteed by a file lease, an etcd lease or
// nothing at all. Adapters assert they satisfy it; this package must not import them.
type SingleIbsenWriterLock interface {
	AcquireLock() bool
	ReleaseLock() bool
}

var _ SingleIbsenWriterLock = &NoFileLock{}

// NoFileLock is the no-op adapter for a single-process or embedded deployment, where there
// is no second writer to fence off.
type NoFileLock struct{}

func (nfl NoFileLock) AcquireLock() bool {
	return true
}

func (nfl NoFileLock) ReleaseLock() bool {
	return true
}

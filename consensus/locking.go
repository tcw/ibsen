package consensus

import (
	"github.com/tcw/ibsen/access/locking"
)

type SingleIbsenWriterLock interface {
	AcquireLock() bool
	ReleaseLock() bool
}

var _ SingleIbsenWriterLock = &NoFileLock{}
var _ SingleIbsenWriterLock = &locking.FileLock{}

type NoFileLock struct{}

func (nfl NoFileLock) AcquireLock() bool {
	return true
}

func (nfl NoFileLock) ReleaseLock() bool {
	return true
}

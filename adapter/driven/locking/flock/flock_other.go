//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package flock

import "fmt"

// Supported says this platform has flock. Here it does not, so auto picks the lease file.
const Supported = false

// Lock exists here only so the package builds; New's lock refuses every acquire.
type Lock struct{ path string }

func New(path string) *Lock { return &Lock{path: path} }

func (l *Lock) ID() string { return "" }

func (l *Lock) Acquire() error {
	return fmt.Errorf("%w: this platform has no flock", ErrNotGuaranteed)
}

func (l *Lock) AcquireLock() bool { return false }
func (l *Lock) ReleaseLock() bool { return false }

func Guaranteed(dir string) error {
	return fmt.Errorf("%w: this platform has no flock", ErrNotGuaranteed)
}

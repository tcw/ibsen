// Package faultfs injects storage faults into an afero filesystem, so a crash can be
// tested where it actually happens: below the adapter, in the media. It is test support
// that ships in the tree because both the adapter's own tests and the core's crash
// recovery tests need the same faults.
package faultfs

import (
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/spf13/afero"
)

// ErrCrashed is what every operation gives once the process is considered gone.
var ErrCrashed = errors.New("the process crashed")

// CrashFs lets a single write land only partly on the media and then fails everything, as
// a filesystem does for a process that is no longer running. Restart brings the media back
// with exactly the bytes that survived, which is what a restarted server finds.
type CrashFs struct {
	afero.Fs
	mu        sync.Mutex
	armed     bool
	suffix    string
	allowance int
	crashed   bool
}

func NewCrash(base afero.Fs) *CrashFs {
	return &CrashFs{Fs: base}
}

// ArmAfter makes the next write put at most allowance bytes on the media, after which the
// process is gone: that write and everything after it fails until Restart.
func (c *CrashFs) ArmAfter(allowance int) {
	c.ArmAfterFor("", allowance)
}

// ArmAfterFor is ArmAfter for the next write to a file whose name ends in suffix, so a
// crash can be aimed at one kind of block. An empty suffix matches every file.
func (c *CrashFs) ArmAfterFor(suffix string, allowance int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
	c.suffix = suffix
	c.allowance = allowance
}

// Crashed reports whether the crash has happened.
func (c *CrashFs) Crashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

// Restart makes the media usable again, holding whatever survived the crash.
func (c *CrashFs) Restart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = false
	c.crashed = false
}

// writeAllowance reports how many of n bytes may still land, and whether this write is the
// one the process does not come back from.
func (c *CrashFs) writeAllowance(name string, n int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return 0, true
	}
	if !c.armed || !strings.HasSuffix(name, c.suffix) {
		return n, false
	}
	c.armed = false
	c.crashed = true
	if c.allowance < n {
		return c.allowance, true
	}
	return n, true
}

func (c *CrashFs) isCrashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

func (c *CrashFs) Open(name string) (afero.File, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.wrap(c.Fs.Open(name))
}

func (c *CrashFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.wrap(c.Fs.OpenFile(name, flag, perm))
}

func (c *CrashFs) Create(name string) (afero.File, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.wrap(c.Fs.Create(name))
}

func (c *CrashFs) Mkdir(name string, perm os.FileMode) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.Fs.Mkdir(name, perm)
}

func (c *CrashFs) MkdirAll(path string, perm os.FileMode) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.Fs.MkdirAll(path, perm)
}

func (c *CrashFs) Remove(name string) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.Fs.Remove(name)
}

func (c *CrashFs) Stat(name string) (os.FileInfo, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.Fs.Stat(name)
}

func (c *CrashFs) wrap(file afero.File, err error) (afero.File, error) {
	if err != nil {
		return nil, err
	}
	return &crashFile{File: file, fs: c}, nil
}

type crashFile struct {
	afero.File
	fs *CrashFs
}

func (f *crashFile) Write(p []byte) (int, error) {
	allowed, crashes := f.fs.writeAllowance(f.File.Name(), len(p))
	if !crashes {
		return f.File.Write(p)
	}
	n := 0
	if allowed > 0 {
		n, _ = f.File.Write(p[:allowed])
	}
	return n, ErrCrashed
}

func (f *crashFile) Truncate(size int64) error {
	if f.fs.isCrashed() {
		return ErrCrashed
	}
	return f.File.Truncate(size)
}

func (f *crashFile) Sync() error {
	if f.fs.isCrashed() {
		return ErrCrashed
	}
	return f.File.Sync()
}

func (f *crashFile) Read(p []byte) (int, error) {
	if f.fs.isCrashed() {
		return 0, ErrCrashed
	}
	return f.File.Read(p)
}

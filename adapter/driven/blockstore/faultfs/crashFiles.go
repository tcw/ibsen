package faultfs

import (
	"os"
	"strings"
	"sync"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
)

// CrashFiles is CrashFs for the filestore adapter's own filesystem seam. Same faults, same
// API; it wraps a filestore.FS rather than an afero one, and it does not need the filesystem
// to tell it a file's name because it knew the name when it opened it.
type CrashFiles struct {
	base filestore.FS

	mu        sync.Mutex
	armed     bool
	suffix    string
	allowance int
	crashed   bool
}

var _ filestore.FS = &CrashFiles{}

// NewCrashFiles wraps a filesystem so its writes can be torn.
func NewCrashFiles(base filestore.FS) *CrashFiles {
	return &CrashFiles{base: base}
}

// ArmAfter makes the next write put at most allowance bytes on the media, after which the
// process is gone: that write and everything after it fails until Restart.
func (c *CrashFiles) ArmAfter(allowance int) {
	c.ArmAfterFor("", allowance)
}

// ArmAfterFor is ArmAfter for the next write to a file whose name ends in suffix, so a crash
// can be aimed at one kind of block. An empty suffix matches every file.
func (c *CrashFiles) ArmAfterFor(suffix string, allowance int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
	c.suffix = suffix
	c.allowance = allowance
}

// Crashed reports whether the crash has happened.
func (c *CrashFiles) Crashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

// Restart makes the media usable again, holding whatever survived the crash.
func (c *CrashFiles) Restart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = false
	c.crashed = false
}

// writeAllowance reports how many of n bytes may still land, and whether this write is the
// one the process does not come back from.
func (c *CrashFiles) writeAllowance(name string, n int) (int, bool) {
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

func (c *CrashFiles) isCrashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

func (c *CrashFiles) Stat(name string) (os.FileInfo, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.base.Stat(name)
}

func (c *CrashFiles) ReadDir(name string) ([]os.FileInfo, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	return c.base.ReadDir(name)
}

func (c *CrashFiles) Mkdir(name string, perm os.FileMode) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.base.Mkdir(name, perm)
}

func (c *CrashFiles) MkdirAll(name string, perm os.FileMode) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.base.MkdirAll(name, perm)
}

func (c *CrashFiles) Remove(name string) error {
	if c.isCrashed() {
		return ErrCrashed
	}
	return c.base.Remove(name)
}

func (c *CrashFiles) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	if c.isCrashed() {
		return nil, ErrCrashed
	}
	file, err := c.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &crashFileHandle{File: file, name: name, fs: c}, nil
}

type crashFileHandle struct {
	filestore.File
	name string
	fs   *CrashFiles
}

func (f *crashFileHandle) Write(p []byte) (int, error) {
	allowed, crashes := f.fs.writeAllowance(f.name, len(p))
	if !crashes {
		return f.File.Write(p)
	}
	n := 0
	if allowed > 0 {
		n, _ = f.File.Write(p[:allowed])
	}
	return n, ErrCrashed
}

func (f *crashFileHandle) Truncate(size int64) error {
	if f.fs.isCrashed() {
		return ErrCrashed
	}
	return f.File.Truncate(size)
}

func (f *crashFileHandle) Sync() error {
	if f.fs.isCrashed() {
		return ErrCrashed
	}
	return f.File.Sync()
}

func (f *crashFileHandle) Read(p []byte) (int, error) {
	if f.fs.isCrashed() {
		return 0, ErrCrashed
	}
	return f.File.Read(p)
}

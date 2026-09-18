package filestore

import (
	"io"
	"os"
)

// FS is the little of a filesystem this adapter uses, and it exists for one reason: a test
// has to be able to put faults underneath the store, and a torn write is not something you
// can ask a real disk for.
//
// It is not a filesystem abstraction and it is not a port. It is the exact set of calls the
// store below makes, it has two implementations — the real one and a faulty one — and neither
// reaches outside the standard library. That is the whole difference between this and the
// general-purpose abstraction it replaces, which brought an HTTP filesystem and unicode
// normalisation along in order to read a file.
type FS interface {
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.FileInfo, error)
	Mkdir(name string, perm os.FileMode) error
	MkdirAll(name string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Remove(name string) error
}

// File is the little of an open file this adapter uses. *os.File satisfies it as it stands.
type File interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

var _ FS = OS{}

// OS is the filesystem the server runs on: the standard library, with nothing in between.
type OS struct{}

func (OS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

func (OS) Mkdir(name string, perm os.FileMode) error { return os.Mkdir(name, perm) }

func (OS) MkdirAll(name string, perm os.FileMode) error { return os.MkdirAll(name, perm) }

func (OS) Remove(name string) error { return os.Remove(name) }

func (OS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		// a typed nil in a File would not compare equal to nil at the call site
		return nil, err
	}
	return file, nil
}

// ReadDir lists a directory with the size of each entry, which is what listing blocks needs.
func (OS) ReadDir(name string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			// a file that went away between listing the directory and asking about it is
			// simply not there, which is not this store's business
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

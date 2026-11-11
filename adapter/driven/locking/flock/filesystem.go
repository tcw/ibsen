package flock

// Filesystem is what Guaranteed knows about the filesystem a directory is on.
type Filesystem struct {
	// Name is the filesystem's type, as the platform names it, or its magic number.
	Name string
	// Local is true for a type on the allow-list: local filesystems known to honour flock.
	Local bool
	// Remote is true for a type known to be a network or remote filesystem. A type that is
	// neither is unknown, and an unknown type is not guaranteed either.
	Remote bool
}

// filesystemOf is a variable so a test can put the data directory on NFS.
var filesystemOf = statFilesystem

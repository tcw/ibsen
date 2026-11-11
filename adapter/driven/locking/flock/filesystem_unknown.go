//go:build !(linux || darwin || freebsd)

package flock

import "errors"

// On the remaining platforms the filesystem type is not read, so nothing is guaranteed and
// auto picks the lease file.
func statFilesystem(dir string) (Filesystem, error) {
	return Filesystem{}, errors.New("the filesystem type is not read on this platform")
}

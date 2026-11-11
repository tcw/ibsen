//go:build darwin || freebsd

package flock

import "syscall"

func statFilesystem(dir string) (Filesystem, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return Filesystem{}, err
	}
	var name []byte
	for _, c := range stat.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	return classifyByName(string(name)), nil
}

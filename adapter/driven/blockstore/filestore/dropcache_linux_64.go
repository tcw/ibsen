//go:build linux && (amd64 || arm64)

package filestore

import "syscall"

// fadviseDontNeed asks the kernel to drop the file's clean cached pages, which it does for
// every page not dirty or under writeback: posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED).
func fadviseDontNeed(fd uintptr) error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, fd, 0, 0, posixFadvDontNeed, 0, 0); errno != 0 {
		return errno
	}
	return nil
}

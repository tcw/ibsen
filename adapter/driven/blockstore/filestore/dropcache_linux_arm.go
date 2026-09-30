//go:build linux && arm

package filestore

import "syscall"

// fadviseDontNeed is posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED). On 32-bit ARM the call
// takes the advice second, so the 64-bit offset and length can sit in aligned register pairs.
func fadviseDontNeed(fd uintptr) error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_ARM_FADVISE64_64, fd, posixFadvDontNeed, 0, 0, 0, 0); errno != 0 {
		return errno
	}
	return nil
}

package flock

import (
	"fmt"
	"syscall"
)

// Magic numbers from statfs(2) and linux/magic.h.
var linuxLocal = map[int64]string{
	0xEF53:     "ext4", // also ext2 and ext3, which share it
	0x58465342: "xfs",
	0x9123683E: "btrfs",
	0x01021994: "tmpfs",
	0x2FC12FC1: "zfs",
	0x794C7630: "overlayfs",
}

var linuxRemote = map[int64]string{
	0x6969:     "nfs",
	0x517B:     "smb",
	0xFF534D42: "cifs",
	0xFE534D42: "smb2",
	0x00C36400: "cephfs",
	0x65735546: "fuse", // GlusterFS, sshfs and the rest of FUSE look the same from here
	0x01161970: "gfs2",
	0x7461636F: "ocfs2",
	0x5346414F: "afs",
	0x6B414653: "kafs",
	0x47504653: "gpfs",
	0x0BD00BD0: "lustre",
	0x013111A8: "ibrix",
	0x19830326: "fhgfs",
	0xAAD7AAEA: "panfs",
	0x564C:     "ncp",
	0x9FA1:     "openprom",
}

func statFilesystem(dir string) (Filesystem, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return Filesystem{}, err
	}
	magic := int64(stat.Type) & 0xFFFFFFFF
	if name, ok := linuxLocal[magic]; ok {
		return Filesystem{Name: name, Local: true}, nil
	}
	if name, ok := linuxRemote[magic]; ok {
		return Filesystem{Name: name, Remote: true}, nil
	}
	return Filesystem{Name: fmt.Sprintf("an unknown filesystem (magic 0x%X)", magic)}, nil
}

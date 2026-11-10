package flock

// Filesystem type names as macOS and the BSDs report them in f_fstypename.
var namedLocal = map[string]bool{
	"apfs": true, "hfs": true, "ufs": true, "ffs": true, "zfs": true, "tmpfs": true,
	"ext4": true, "xfs": true, "btrfs": true,
}

var namedRemote = map[string]bool{
	"nfs": true, "smbfs": true, "cifs": true, "afpfs": true, "webdav": true, "fusefs": true,
	"osxfuse": true, "macfuse": true, "ceph": true, "glusterfs": true, "afs": true,
}

func classifyByName(name string) Filesystem {
	return Filesystem{Name: name, Local: namedLocal[name], Remote: namedRemote[name]}
}

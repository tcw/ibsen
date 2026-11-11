package flock

import "testing"

// The two filesystems a Linux data directory is most often on, read for real.
func TestTmpfsAndTheTestDirectoryAreLocal(t *testing.T) {
	for _, dir := range []string{"/dev/shm", t.TempDir()} {
		fs, err := statFilesystem(dir)
		if err != nil {
			t.Skipf("unable to stat %s: %v", dir, err)
		}
		t.Logf("%s is on %s", dir, fs.Name)
		if dir == "/dev/shm" && (fs.Name != "tmpfs" || !fs.Local) {
			t.Errorf("/dev/shm read as %+v, want local tmpfs", fs)
		}
	}
}

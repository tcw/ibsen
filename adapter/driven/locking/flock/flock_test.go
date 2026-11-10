//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flock

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// helperEnv makes the test binary a second process holding the lock, which is the only honest
// way to test a lock between processes: flock locks belong to open file descriptions, so a
// second process is a second holder in every sense the kernel cares about.
const helperEnv = "IBSEN_FLOCK_TEST_HOLD"

func TestMain(m *testing.M) {
	if path := os.Getenv(helperEnv); path != "" {
		lock := New(path)
		if err := lock.Acquire(); err != nil {
			os.Stdout.WriteString("refused " + err.Error() + "\n")
			os.Exit(2)
		}
		os.Stdout.WriteString("locked\n")
		// hold it until killed; a kill is what a crash looks like to the kernel
		select {}
	}
	os.Exit(m.Run())
}

func holdInAnotherProcess(t *testing.T, path string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), helperEnv+"="+path)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "locked" {
		t.Fatalf("the helper did not take the lock: %q, %v", line, err)
	}
	return cmd
}

func lockPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := Guaranteed(dir); err != nil {
		t.Skipf("the test directory cannot hold a kernel lock here: %v", err)
	}
	return filepath.Join(dir, ".writeLock")
}

func TestAcquireWritesTheHolderAndReleaseRemovesIt(t *testing.T) {
	path := lockPath(t)
	lock := New(path)
	if err := lock.Acquire(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != lock.ID() {
		t.Errorf("the lock file holds %q, want the holder's id %q", content, lock.ID())
	}
	if !lock.ReleaseLock() {
		t.Fatal("release failed")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("release left the lock file behind: %v", err)
	}
	if err := New(path).Acquire(); err != nil {
		t.Errorf("a released lock could not be taken again: %v", err)
	}
}

func TestAHolderInThisProcessRefusesASecond(t *testing.T) {
	path := lockPath(t)
	first := New(path)
	if err := first.Acquire(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.ReleaseLock() })
	if err := New(path).Acquire(); !errors.Is(err, ErrHeld) {
		t.Fatalf("a second lock on a held file got %v, want ErrHeld", err)
	}
	if err := first.Acquire(); !errors.Is(err, ErrHeld) {
		t.Fatalf("acquiring a lock already held got %v, want ErrHeld", err)
	}
}

func TestAnotherProcessHoldingItIsRefusedAndItsDeathReleasesIt(t *testing.T) {
	path := lockPath(t)
	helper := holdInAnotherProcess(t, path)

	if err := New(path).Acquire(); !errors.Is(err, ErrHeld) {
		t.Fatalf("got %v while another process holds the lock, want ErrHeld", err)
	}

	// no release, no clean exit: the kernel is what lets go
	if err := helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = helper.Wait()

	lock := New(path)
	if err := lock.Acquire(); err != nil {
		t.Fatalf("the lock of a killed process was not released: %v", err)
	}
	lock.ReleaseLock()
}

func TestALiveLeaseInTheFileIsRefused(t *testing.T) {
	path := lockPath(t)
	if err := os.WriteFile(path, []byte("8a2f0c1e-lease-file-holder"), 0660); err != nil {
		t.Fatal(err)
	}
	if err := New(path).Acquire(); !errors.Is(err, ErrHeld) {
		t.Fatalf("got %v over a lease renewed a moment ago, want ErrHeld", err)
	}

	// a lease nobody renewed for longer than it is granted for has no holder
	old := time.Now().Add(-2 * LeaseTime)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	lock := New(path)
	if err := lock.Acquire(); err != nil {
		t.Fatalf("an expired lease was not taken over: %v", err)
	}
	lock.ReleaseLock()
}

func TestAFlockHolderThatDiedLeavesNoLease(t *testing.T) {
	path := lockPath(t)
	// what a killed flock holder leaves: its id in a file nobody has locked
	if err := os.WriteFile(path, []byte("flock-0123456789abcdef"), 0660); err != nil {
		t.Fatal(err)
	}
	lock := New(path)
	if err := lock.Acquire(); err != nil {
		t.Fatalf("a dead flock holder's file was read as a live lease: %v", err)
	}
	lock.ReleaseLock()
}

func TestTheSelfTestCatchesAFilesystemThatIgnoresTheLock(t *testing.T) {
	path := lockPath(t)
	saved := secondHolderRefused
	t.Cleanup(func() { secondHolderRefused = saved })
	secondHolderRefused = func(string) (bool, error) { return false, nil }

	err := New(path).Acquire()
	if !errors.Is(err, ErrNotGuaranteed) {
		t.Fatalf("got %v from a filesystem that granted the lock twice, want ErrNotGuaranteed", err)
	}
	if !strings.Contains(err.Error(), "not enforcing") {
		t.Errorf("the error does not say why: %v", err)
	}
	// whatever takes over must not find an empty file it reads as a holder
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the failed attempt left an empty lock file: %v", err)
	}
}

func TestGuaranteedRefusesRemoteAndUnknownFilesystems(t *testing.T) {
	saved := filesystemOf
	t.Cleanup(func() { filesystemOf = saved })

	cases := []struct {
		fs   Filesystem
		ok   bool
		says string
	}{
		{Filesystem{Name: "ext4", Local: true}, true, ""},
		{Filesystem{Name: "tmpfs", Local: true}, true, ""},
		{Filesystem{Name: "nfs", Remote: true}, false, "network or remote"},
		{Filesystem{Name: "cifs", Remote: true}, false, "cifs"},
		{Filesystem{Name: "an unknown filesystem (magic 0x1234)"}, false, "not on the list"},
	}
	for _, c := range cases {
		filesystemOf = func(string) (Filesystem, error) { return c.fs, nil }
		err := Guaranteed("/srv/data")
		if c.ok {
			if err != nil {
				t.Errorf("%s: got %v, want guaranteed", c.fs.Name, err)
			}
			continue
		}
		if !errors.Is(err, ErrNotGuaranteed) || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: got %v, want ErrNotGuaranteed saying %q", c.fs.Name, err, c.says)
		}
	}

	filesystemOf = func(string) (Filesystem, error) { return Filesystem{}, errors.New("statfs failed") }
	if err := Guaranteed("/srv/data"); !errors.Is(err, ErrNotGuaranteed) {
		t.Errorf("a filesystem that cannot be read got %v, want ErrNotGuaranteed", err)
	}
}

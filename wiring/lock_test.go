package wiring

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/core/port/driven"
)

// The CLI used to build the lease itself, at <root>/.writeLock. The composition root now
// does, and it has to land in the same place: an operator's existing data directory already
// holds one, and another instance is fenced off by that exact path.
//
// The directory is a real one because the lease is. The lock adapter reaches the filesystem
// through the standard library rather than through whatever this server was handed, which is
// what a lease fencing off another process has to do; a lease on a filesystem that exists only
// inside this process fences off nobody.
func TestStartBuildsTheWriteLockInTheDataDirectory(t *testing.T) {
	root := t.TempDir()
	ibs := &IbsenServer{RootPath: root}

	ibs.defaults()

	if ibs.Lock == nil {
		t.Fatal("no lock was built for a server that was given none")
	}
	if !ibs.Lock.AcquireLock() {
		t.Fatal("the lock that was built could not be acquired")
	}
	t.Cleanup(func() { ibs.Lock.ReleaseLock() })

	if _, err := os.Stat(filepath.Join(root, ".writeLock")); err != nil {
		t.Errorf("acquiring the lock did not create <root>/.writeLock: %v", err)
	}
}

// A caller that brings its own coordination adapter keeps it; defaults only fills gaps.
func TestAnInjectedLockIsKept(t *testing.T) {
	injected := &slowReleaseLock{released: make(chan struct{})}
	ibs := &IbsenServer{
		Afs:      &afero.Afero{Fs: afero.NewMemMapFs()},
		RootPath: "/data",
		Lock:     injected,
	}

	ibs.defaults()

	if ibs.Lock != injected {
		t.Errorf("defaults replaced an injected lock with %T", ibs.Lock)
	}
}

// refusingLock stands in for another instance already holding the lease.
type refusingLock struct{ acquireCalls int }

func (l *refusingLock) AcquireLock() bool {
	l.acquireCalls++
	return false
}

func (l *refusingLock) ReleaseLock() bool { return true }

// Start used to log.Fatal here, which exits the process from inside a library call: a
// program embedding the log had no say. It returns the refusal instead.
func TestStartReturnsWhenAnotherInstanceHoldsTheLock(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	lock := &refusingLock{}
	ibs := &IbsenServer{Lock: lock, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	started := make(chan error, 1)
	go func() { started <- ibs.Start(lis) }()

	select {
	case err := <-started:
		if err == nil {
			t.Fatal("Start returned no error although the lock was refused")
		}
		if !errors.Is(err, ErrWriteLockUnavailable) {
			t.Errorf("error does not match ErrWriteLockUnavailable: %v", err)
		}
		if !strings.Contains(err.Error(), "/data") {
			t.Errorf("error does not say which path it could not lock: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the lock was refused")
	}

	if lock.acquireCalls != 1 {
		t.Errorf("AcquireLock was called %d times, want 1", lock.acquireCalls)
	}
}

// A read-only server writes nothing, so it never takes the lease and a held lock does not
// stop it starting.
func TestReadonlyStartDoesNotTakeTheLock(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lock := &refusingLock{}
	ibs := &IbsenServer{Readonly: true, Lock: lock, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	started := make(chan error, 1)
	go func() { started <- ibs.Start(lis) }()

	// it got past the lock check if it is still serving
	select {
	case err := <-started:
		t.Fatalf("a read-only Start returned early: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	ibs.ShutdownCleanly()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after a clean shutdown")
	}

	if lock.acquireCalls != 0 {
		t.Errorf("a read-only server took the write lock %d times", lock.acquireCalls)
	}
}

// Compression is a name on the command line and an adapter in here: the CLI is a driving
// adapter and must not reach a driven one, so the composition root is what turns one into
// the other.
func TestCompressionNamePicksTheCodec(t *testing.T) {
	for _, test := range []struct {
		name string
		want driven.CodecID
	}{
		{name: "", want: driven.CodecNone},
		{name: "none", want: driven.CodecNone},
		{name: "zstd", want: driven.CodecZstd},
	} {
		t.Run(test.name, func(t *testing.T) {
			ibsen := &IbsenServer{Compression: test.name}
			if err := ibsen.resolveCodecs(); err != nil {
				t.Fatal(err)
			}
			defer ibsen.zstdCodec.Close()
			if got := ibsen.Codec.ID(); got != test.want {
				t.Errorf("compression %q writes with %s, want %s", test.name, got, test.want)
			}
		})
	}
}

// A name with no adapter behind it is refused rather than quietly becoming no compression,
// which would write a log the operator did not ask for.
func TestAnUnknownCompressionNameIsRefused(t *testing.T) {
	ibsen := &IbsenServer{Compression: "snappy"}

	err := ibsen.resolveCodecs()

	if !errors.Is(err, ErrUnknownCompression) {
		t.Fatalf("got %v, want ErrUnknownCompression", err)
	}
	if ibsen.zstdCodec != nil {
		ibsen.zstdCodec.Close()
	}
}

func TestAnUnknownCompressionLevelIsRefused(t *testing.T) {
	ibsen := &IbsenServer{Compression: "zstd", CompressionLevel: "turbo"}

	if err := ibsen.resolveCodecs(); err == nil {
		t.Fatal("an unknown compression level was accepted")
	}
}

// Every codec the binary links is readable whatever it writes with, so turning compression
// off never strands a block written while it was on.
func TestTheReadRegistryHoldsEveryLinkedCodec(t *testing.T) {
	ibsen := &IbsenServer{Compression: "none"}
	if err := ibsen.resolveCodecs(); err != nil {
		t.Fatal(err)
	}
	defer ibsen.zstdCodec.Close()

	for _, id := range []driven.CodecID{driven.CodecNone, driven.CodecZstd} {
		codec, err := ibsen.Codecs.Get(id)
		if err != nil {
			t.Errorf("a server writing no compression cannot read %s: %v", id, err)
			continue
		}
		if codec.ID() != id {
			t.Errorf("%s resolved to %s", id, codec.ID())
		}
	}
}

// An embedder that brings its own codec keeps it, and it is readable.
func TestAnInjectedCodecIsKeptAndReadable(t *testing.T) {
	injected := stubCodec{}
	ibsen := &IbsenServer{Compression: "zstd", Codec: injected}
	if err := ibsen.resolveCodecs(); err != nil {
		t.Fatal(err)
	}
	defer ibsen.zstdCodec.Close()

	if ibsen.Codec != driven.Codec(injected) {
		t.Errorf("an injected codec was replaced by %s", ibsen.Codec.ID())
	}
	if codec, err := ibsen.Codecs.Get(injected.ID()); err != nil || codec.ID() != injected.ID() {
		t.Errorf("an injected codec is not in the read registry: %v, %v", codec, err)
	}
}

type stubCodec struct{}

func (stubCodec) ID() driven.CodecID { return driven.CodecID(77) }
func (stubCodec) Encode(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (stubCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src...), nil
}

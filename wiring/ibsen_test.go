package wiring

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// slowReleaseLock is a single-writer lock that runs onRelease before reporting the release.
type slowReleaseLock struct {
	onRelease func()
	released  chan struct{}
}

func (l *slowReleaseLock) AcquireLock() bool {
	return true
}

func (l *slowReleaseLock) ReleaseLock() bool {
	l.onRelease()
	close(l.released)
	return true
}

func TestShutdown_closesLogBeforeReleasingLockAndStartWaitsForIt(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lock := &slowReleaseLock{released: make(chan struct{})}
	ibs := &IbsenServer{Lock: lock, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}
	var writeAtRelease error
	lock.onRelease = func() {
		ibs.mu.Lock()
		topicsManager := ibs.topicsManager
		ibs.mu.Unlock()
		entries := [][]byte{[]byte("late")}
		writeAtRelease = topicsManager.Write("topic", &entries)
		// releasing takes a while, so a Start that does not wait returns first
		time.Sleep(100 * time.Millisecond)
	}
	started := make(chan error, 1)
	go func() {
		started <- ibs.Start(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpcapi.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("server did not start: %v", err)
	}
	_, err = grpcapi.NewIbsenClient(conn).Write(ctx, &grpcapi.InputEntries{Topic: "topic", Entries: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	go ibs.ShutdownCleanly()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after shutdown")
	}
	select {
	case <-lock.released:
	default:
		t.Fatal("Start returned before the lock was released")
	}
	if !errors.Is(writeAtRelease, manager.ErrClosed) {
		t.Fatalf("write while releasing the lock: err=%v, want ErrClosed", writeAtRelease)
	}
}

// startInMemory runs an in-memory server and returns a client to it, plus its shutdown.
func startInMemory(t *testing.T, ibs *IbsenServer) (grpcapi.IbsenClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- ibs.Start(lis) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := grpcapi.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		t.Fatalf("server did not start: %v", err)
	}
	return grpcapi.NewIbsenClient(conn), func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		cancel()
		go ibs.ShutdownCleanly()
		select {
		case err := <-started:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Start did not return after shutdown")
		}
	}
}

// In-memory mode keeps the log in a memstore, so it reaches no filesystem at all. It used to
// run the filesystem adapter over an emulated filesystem, which wrote real blocks into a fake
// disk to avoid writing to a real one.
func TestInMemoryModeWritesToNoFilesystem(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	ibs := &IbsenServer{InMemory: true, Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	client, shutdown := startInMemory(t, ibs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Write(ctx, &grpcapi.InputEntries{
		Topic: "topic", Entries: [][]byte{[]byte("one"), []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	shutdown()

	var found []string
	err := afero.Walk(afs, "/", func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("in-memory mode wrote %v to a filesystem", found)
	}
}

// And it needs no filesystem to be given to it at all, which is the point: a memstore is not
// a filesystem, so there is nothing to emulate one with.
func TestInMemoryModeNeedsNoFilesystem(t *testing.T) {
	ibs := &IbsenServer{InMemory: true, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	client, shutdown := startInMemory(t, ibs)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Write(ctx, &grpcapi.InputEntries{
		Topic: "topic", Entries: [][]byte{[]byte("one")},
	}); err != nil {
		t.Fatal(err)
	}
	topics, err := client.List(ctx, &grpcapi.EmptyArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if len(topics.GetTopics()) != 1 || topics.GetTopics()[0] != "topic" {
		t.Errorf("listed %v, want the one topic written", topics.GetTopics())
	}
}

// There is no second writer to fence off when the log lives in this process, and no
// filesystem to keep a lease on.
func TestInMemoryModeTakesNoWriteLock(t *testing.T) {
	ibs := &IbsenServer{InMemory: true, RootPath: "/data"}

	ibs.defaults()

	if _, isNoLock := ibs.Lock.(driven.NoFileLock); !isNoLock {
		t.Errorf("in-memory mode built a %T, want driven.NoFileLock", ibs.Lock)
	}
}

// The on-disk path is unchanged: it still keeps the log on the filesystem it was given.
func TestOnDiskModeStillWritesToTheFilesystem(t *testing.T) {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	if err := afs.MkdirAll("/data", 0700); err != nil {
		t.Fatal(err)
	}
	ibs := &IbsenServer{Lock: &slowReleaseLock{released: make(chan struct{}), onRelease: func() {}},
		Afs: afs, RootPath: "/data", TTL: time.Minute, MaxBlockSize: 1000}

	client, shutdown := startInMemory(t, ibs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Write(ctx, &grpcapi.InputEntries{
		Topic: "topic", Entries: [][]byte{[]byte("one")},
	}); err != nil {
		t.Fatal(err)
	}
	shutdown()

	exists, err := afs.Exists("/data/topic/00000000000000000000.log")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("the on-disk log block was not written")
	}
}

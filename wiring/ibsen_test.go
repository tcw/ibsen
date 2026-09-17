package wiring

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
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
	conn, err := grpc.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
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

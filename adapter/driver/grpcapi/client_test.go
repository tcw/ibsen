package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// listenerWithNothingBehindIt returns an address no one is serving on, by taking a port and
// giving it straight back.
func listenerWithNothingBehindIt(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestDialContextReturnsAConnectedClient(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("unable to connect to a server that is listening: %v", err)
	}
	defer conn.Close()

	if state := conn.GetState().String(); state != "READY" {
		t.Errorf("connection state is %s, want READY", state)
	}
}

// This is the whole reason the helper exists. grpc.NewClient connects lazily, so a plain
// call would hand back a healthy-looking client for a server that is not there and fail at
// the first RPC instead.
func TestDialContextFailsWhenNothingIsListening(t *testing.T) {
	addr := listenerWithNothingBehindIt(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, err := DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err == nil {
		conn.Close()
		t.Fatal("connecting to an address nothing is listening on returned no error")
	}
	if conn != nil {
		t.Error("a failed connect returned a non-nil client, which the caller would leak")
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("waited %v for a deadline of 500ms", waited)
	}
}

// The context bounds the wait, so a caller cannot hang on an unreachable server the way
// grpc.Dial with WithBlock and no context used to.
func TestDialContextStopsOnAnAlreadyCancelledContext(t *testing.T) {
	addr := listenerWithNothingBehindIt(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
		t.Fatal("a cancelled context still produced a client")
	}
}

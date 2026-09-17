package grpcapi

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/tcw/ibsen/errore"
)

// DialContext creates a client for target and waits until it is connected, or until ctx is
// done.
//
// grpc.NewClient replaced grpc.Dial, and with it grpc.WithBlock, which is now a no-op: a
// client connects lazily, so one built for a server that is not listening comes back healthy
// and fails at the first call. Callers here want the opposite — a CLI should say it cannot
// reach the server, and a test should not start asserting before the server answers — so
// this waits for the connection the way WithBlock used to.
func DialContext(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, errore.WrapWithContextF(err, "unable to create a client for %s", target)
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if state == connectivity.Shutdown {
			_ = conn.Close()
			return nil, errore.NewF("connection to %s shut down before it was ready", target)
		}
		if state == connectivity.Idle {
			// a failed attempt can drop the channel back to idle; ask it to try again
			conn.Connect()
		}
		if !conn.WaitForStateChange(ctx, state) {
			_ = conn.Close()
			return nil, errore.WrapWithContextF(ctx.Err(), "unable to connect to %s", target)
		}
	}
}

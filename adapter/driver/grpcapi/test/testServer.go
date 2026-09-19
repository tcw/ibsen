package test

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
)

// ibsenTestTarget is the address of the server started by the running test.
var ibsenTestTarget string

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// startTestServer starts an Ibsen gRPC server with an in-memory filesystem on a free local
// port, points the test clients at it, and stops it when the test ends. Each test gets its
// own server, so a test that leaves streams open cannot affect the next one.
func startTestServer(t *testing.T) {
	t.Helper()
	rootPath := t.TempDir()
	// how long a tailing read waits for new entries and how often it looks: a gRPC concern,
	// so it is named here rather than borrowed from the manager's parameters
	const readTTL = 5 * time.Second
	const checkForNewEvery = 100 * time.Millisecond
	topicsManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		ReadOnly:     false,
		Store:        filestore.NewOS(rootPath),
		MaxBlockSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := grpcapi.NewUnsecureIbsenGrpcServer(&topicsManager, readTTL, checkForNewEvery)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	ibsenTestTarget = lis.Addr().String()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := server.StartGRPC(lis); err != nil {
			log.Error().Err(err).Msg("test server failed")
		}
	}()
	// wait until the server answers, so Shutdown never runs before StartGRPC has created it
	client, err := newIbsenClient(ibsenTestTarget)
	if err != nil {
		t.Fatalf("test server did not start: %v", err)
	}
	client.Close()

	t.Cleanup(func() {
		server.Shutdown()
		<-stopped
	})
}

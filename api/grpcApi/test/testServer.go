package test

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/manager"
	"net"
	"os"
	"sync"
	"testing"
	"time"
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
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	rootPath := "/tmp/data"
	if err := afs.MkdirAll(rootPath, 0700); err != nil {
		t.Fatal(err)
	}
	params := manager.LogTopicManagerParams{
		ReadOnly:         false,
		Afs:              afs,
		TTL:              5 * time.Second,
		CheckForNewEvery: 100 * time.Millisecond,
		MaxBlockSize:     10,
		RootPath:         rootPath,
	}
	topicsManager, err := manager.NewLogTopicsManager(params)
	if err != nil {
		t.Fatal(err)
	}
	server := grpcApi.NewUnsecureIbsenGrpcServer(&topicsManager, params.TTL, params.CheckForNewEvery)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	ibsenTestTarget = lis.Addr().String()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		var wg sync.WaitGroup
		if err := server.StartGRPC(lis, &wg, ""); err != nil {
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

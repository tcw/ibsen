package test

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/manager"
	"net"
	"os"
	"sync"
	"time"
)

var afs *afero.Afero
var ibsenServer *grpcApi.IbsenGrpcServer
var ibsenTestTarge = fmt.Sprintf("%s:%d", "localhost", 50002)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func startGrpcServer(afs *afero.Afero, rootPath string) {
	err := afs.Mkdir(rootPath, 0600)
	if err != nil {
		log.Fatal().Err(err)
	}
	topicsManager, err := manager.NewLogTopicsManager(afs, false, 30*time.Second, 30*time.Second, rootPath, 10)
	if err != nil {
		log.Fatal().Err(err)
	}
	ibsenServer = grpcApi.NewUnsecureIbsenGrpcServer(&topicsManager)
	lis, err := net.Listen("tcp", ibsenTestTarge)
	if err != nil {
		log.Fatal().Err(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	err = ibsenServer.StartGRPC(lis, &wg, "")
	if err != nil {
		log.Fatal().Err(err).Msg("Test server failed")
	}
	wg.Done()
}

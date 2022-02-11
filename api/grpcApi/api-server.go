package grpcApi

import (
	"context"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/testdata"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type server struct {
	manager manager.LogManager
}

type IbsenGrpcServer struct {
	Host        string
	Port        uint16
	CertFile    string
	KeyFile     string
	UseTls      bool
	IbsenServer *grpc.Server
	Manager     manager.LogManager
}

func NewIbsenGrpcServer(manager manager.LogManager) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		Host:     "0.0.0.0",
		Port:     50001,
		CertFile: "",
		KeyFile:  "",
		UseTls:   false,
		Manager:  manager,
	}
}

func (igs *IbsenGrpcServer) StartGRPC(listener net.Listener) error {
	var opts []grpc.ServerOption
	opts = []grpc.ServerOption{
		grpc.ConnectionTimeout(time.Hour * 1),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32)}
	if igs.UseTls {
		absCert := testdata.Path(igs.CertFile)
		absKey := testdata.Path(igs.KeyFile)
		creds, err := credentials.NewServerTLSFromFile(absCert, absKey)
		opts = append(opts, grpc.Creds(creds))
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	grpcServer := grpc.NewServer(opts...)

	igs.IbsenServer = grpcServer

	RegisterIbsenServer(grpcServer, &server{
		manager: igs.Manager,
	})
	return grpcServer.Serve(listener)
}

func (igs *IbsenGrpcServer) Shutdown() {
	igs.IbsenServer.Stop()
}

var _ IbsenServer = &server{}

func (s server) Write(ctx context.Context, entries *InputEntries) (*WriteStatus, error) {
	n, err := s.manager.Write(access.Topic(entries.Topic), &entries.Entries)
	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return nil, status.Error(codes.Unknown, "Error writing batch")
	}
	return &WriteStatus{
		Wrote: int64(n),
	}, nil
}

func (s server) Read(params *ReadParams, readServer Ibsen_ReadServer) error {
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, readServer)
	err := s.manager.Read(access.ReadParams{
		Topic:            access.Topic(params.Topic),
		Offset:           access.Offset(params.Offset),
		StopOnCompletion: params.StopOnCompletion,
		BatchSize:        params.BatchSize,
		LogChan:          logChan,
		Wg:               &wg,
	})

	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return status.Error(codes.Unknown, "Error reading streaming")
	}
	wg.Wait()
	return nil
}

func sendBatchMessage(logChan chan *[]access.LogEntry, wg *sync.WaitGroup, outStream Ibsen_ReadServer) {
	for {
		entryBatch := <-logChan
		batch := *entryBatch
		if len(batch) == 0 {
			break
		}
		err := outStream.Send(&OutputEntries{
			Entries: convert(entryBatch),
		})
		if err != nil {
			err = errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return
		}
		wg.Done()
	}
}

func convert(entries *[]access.LogEntry) []*Entry {
	outEntries := make([]*Entry, len(*entries))
	for i, entry := range *entries {
		outEntries[i] = &Entry{
			Offset:  entry.Offset,
			Content: entry.Entry,
		}
	}
	return outEntries
}

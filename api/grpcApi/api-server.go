package grpcApi

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/manager"
	"github.com/tcw/ibsen/telemetry"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/testdata"
	"math"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

var tracer = otel.Tracer("ibsen-server")

type server struct {
	manager manager.LogManager
}

type IbsenGrpcServer struct {
	CertFile    string
	KeyFile     string
	UseTls      bool
	IbsenServer *grpc.Server
	Manager     manager.LogManager
}

func NewUnsecureIbsenGrpcServer(manager manager.LogManager) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		CertFile: "",
		KeyFile:  "",
		UseTls:   false,
		Manager:  manager,
	}
}

func NewIbsenGrpcServer(manager manager.LogManager, key string, cert string) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		CertFile: cert,
		KeyFile:  key,
		UseTls:   true,
		Manager:  manager,
	}
}

func (igs *IbsenGrpcServer) StartGRPC(listener net.Listener, wg *sync.WaitGroup, OTELExporterAddr string) error {
	if OTELExporterAddr != "" {
		go telemetry.ConnectToOTELExporter(wg, OTELExporterAddr)
	}
	var opts []grpc.ServerOption
	opts = []grpc.ServerOption{
		grpc.ConnectionTimeout(time.Hour * 1),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
	}
	if igs.UseTls {
		absCert := testdata.Path(igs.CertFile)
		absKey := testdata.Path(igs.KeyFile)
		creds, err := credentials.NewServerTLSFromFile(absCert, absKey)
		opts = append(opts, grpc.Creds(creds))
		if err != nil {
			return err
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

func (s server) List(ctx context.Context, empty *empty.Empty) (*TopicList, error) {
	list := s.manager.List()
	return &TopicList{
		Topics: convertTopics(list),
	}, nil
}

func convertTopics(topics []manager.TopicName) []string {
	var sTopic []string
	for _, topic := range topics {
		sTopic = append(sTopic, string(topic))
	}
	return sTopic
}

func (s server) Write(ctx context.Context, entries *InputEntries) (*WriteStatus, error) {
	err := s.manager.Write(manager.TopicName(entries.Topic), &entries.Entries)
	if err != nil {
		log.Err(err)
		return nil, status.Error(codes.Unknown, "Error writing batch")
	}
	return &WriteStatus{
		Wrote: int64(0), //todo
	}, nil
}

func (s server) Read(params *ReadParams, readServer Ibsen_ReadServer) error {
	logChan := make(chan *[]access.LogEntry)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, readServer)
	err := s.manager.Read(manager.ReadParams{
		TopicName:        manager.TopicName(params.Topic),
		From:             access.Offset(params.Offset),
		BatchSize:        params.BatchSize,
		LogChan:          logChan,
		Wg:               &wg,
		StopOnCompletion: params.StopOnCompletion,
	})

	if err != nil {
		log.Err(err)
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
			log.Err(err)
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

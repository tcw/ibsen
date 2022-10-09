package grpcApi

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
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
	manager          manager.LogManager
	CheckForNewEvery time.Duration
	TTL              time.Duration
}

type GRPCSecurity struct {
	CertKeyFile   string
	PrivteKeyFile string
}

type IbsenGrpcServer struct {
	GRPCSecurity     GRPCSecurity
	UseTLS           bool
	ConnectionTTL    time.Duration
	CheckForNewEvery time.Duration
	IbsenServer      *grpc.Server
	Manager          manager.LogManager
}

func NewUnsecureIbsenGrpcServer(manager manager.LogManager, TTL time.Duration, checkForNewEvery time.Duration) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		UseTLS:           false,
		Manager:          manager,
		CheckForNewEvery: checkForNewEvery,
		ConnectionTTL:    TTL,
	}
}

func NewSecureIbsenGrpcServer(manager manager.LogManager, grpcSec GRPCSecurity, TTL time.Duration, checkForNewEvery time.Duration) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		GRPCSecurity:     grpcSec,
		UseTLS:           true,
		Manager:          manager,
		CheckForNewEvery: checkForNewEvery,
		ConnectionTTL:    TTL,
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
	if igs.UseTLS {
		absCert := testdata.Path(igs.GRPCSecurity.CertKeyFile)
		absKey := testdata.Path(igs.GRPCSecurity.PrivteKeyFile)
		creds, err := credentials.NewServerTLSFromFile(absCert, absKey)
		opts = append(opts, grpc.Creds(creds))
		if err != nil {
			return err
		}
	}
	grpcServer := grpc.NewServer(opts...)

	igs.IbsenServer = grpcServer

	RegisterIbsenServer(grpcServer, &server{
		manager:          igs.Manager,
		TTL:              igs.ConnectionTTL,
		CheckForNewEvery: igs.CheckForNewEvery,
	})
	return grpcServer.Serve(listener)
}

func (igs *IbsenGrpcServer) Shutdown() {
	igs.IbsenServer.Stop()
}

var _ IbsenServer = &server{}

func (s server) List(ctx context.Context, empty *EmptyArgs) (*TopicList, error) {
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
		log.Error().Str("stack", errore.SprintStackTraceBd(err)).Err(errore.RootCause(err)).Msgf("write api failed")
		return nil, status.Error(codes.Unknown, "error writing batch")
	}
	return &WriteStatus{
		Wrote: int64(0), //todo
	}, nil
}

func (s server) Read(params *ReadParams, readServer Ibsen_ReadServer) error {
	readTTL := time.Now().Add(s.TTL)
	var nextOffset = access.Offset(params.Offset)
	for time.Until(readTTL) > 0 {
		logChan := make(chan *[]access.LogEntry)
		terminate := make(chan bool)
		lastOffset := make(chan access.Offset)
		var wg sync.WaitGroup
		go sendGRPCMessage(logChan, &wg, readServer, terminate, lastOffset)
		topicName := manager.TopicName(params.Topic)
		err := s.manager.Read(manager.ReadParams{
			TopicName: topicName,
			From:      nextOffset,
			BatchSize: params.BatchSize,
			LogChan:   logChan,
			Wg:        &wg,
		})
		if err == manager.TopicNotFound {
			terminate <- true
			return status.Errorf(codes.NotFound, "Topic %s not found", topicName)
		}
		if err == access.NoEntriesFound {
			terminate <- true
			time.Sleep(s.CheckForNewEvery)
			continue
		}
		if err != nil {
			terminate <- true
			log.Error().Str("stack", errore.SprintStackTraceBd(err)).Err(errore.RootCause(err)).Msgf("read api failed")
			return status.Error(codes.Unknown, "error reading streaming")
		}
		wg.Wait()
		terminate <- true
		nextOffset = <-lastOffset + 1
		readTTL = time.Now().Add(s.TTL)
		if params.StopOnCompletion {
			return nil
		}
	}
	return nil
}

func sendGRPCMessage(logChan chan *[]access.LogEntry, wg *sync.WaitGroup, outStream Ibsen_ReadServer, terminate chan bool, lastOffset chan access.Offset) {
	var lastReadOffset = access.Offset(0)
	for {
		select {
		case <-terminate:
			close(logChan)
			lastOffset <- lastReadOffset
			return
		case entryBatch := <-logChan:
			batch := *entryBatch
			if len(batch) == 0 {
				break
			}
			lastReadOffset = access.Offset(batch[len(batch)-1].Offset)
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

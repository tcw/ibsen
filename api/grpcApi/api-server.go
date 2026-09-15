package grpcApi

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/access/common"
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

func NewUnsecureIbsenGrpcServer(
	manager manager.LogManager,
	TTL time.Duration,
	checkForNewEvery time.Duration) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		UseTLS:           false,
		Manager:          manager,
		CheckForNewEvery: checkForNewEvery,
		ConnectionTTL:    TTL,
	}
}

func NewSecureIbsenGrpcServer(
	manager manager.LogManager,
	grpcSec GRPCSecurity,
	TTL time.Duration,
	checkForNewEvery time.Duration) *IbsenGrpcServer {
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

func (s server) mustEmbedUnimplementedIbsenServer() {
}

func (s server) List(ctx context.Context, empty *EmptyArgs) (*TopicList, error) {
	list := s.manager.List()
	return &TopicList{
		Topics: convertTopics(list),
	}, nil
}

func (s server) Write(ctx context.Context, entries *InputEntries) (*WriteStatus, error) {
	if err := common.ValidateTopicName(common.TopicName(entries.Topic)); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err := s.manager.Write(common.TopicName(entries.Topic), &entries.Entries)
	if err != nil {
		log.Error().Str("stack", errore.SprintStackTraceBd(err)).Err(errore.RootCause(err)).Msgf("write api failed")
		return nil, status.Error(codes.Unknown, "error writing batch")
	}
	return &WriteStatus{
		Wrote: int64(len(entries.Entries)),
	}, nil
}

func (s server) Read(params *ReadParams, readServer Ibsen_ReadServer) error {
	if err := common.ValidateTopicName(common.TopicName(params.Topic)); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx := readServer.Context()
	topicName := common.TopicName(params.Topic)
	nextOffset := common.Offset(params.Offset)
	readTTL := time.Now().Add(s.TTL)
	for time.Until(readTTL) > 0 {
		sentUntil, readErr, sendErr := s.streamFrom(ctx, topicName, nextOffset, params.BatchSize, readServer)
		if sendErr != nil {
			return sendErr
		}
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if readErr == manager.TopicNotFound {
			return status.Errorf(codes.NotFound, "Topic %s not found", topicName)
		}
		if readErr == common.NoEntriesFound {
			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			case <-time.After(s.CheckForNewEvery):
			}
			continue
		}
		if readErr != nil {
			log.Error().Str("stack", errore.SprintStackTraceBd(readErr)).Err(errore.RootCause(readErr)).Msgf("read api failed")
			return status.Error(codes.Unknown, "error reading streaming")
		}
		nextOffset = sentUntil
		// refresh ttl
		readTTL = time.Now().Add(s.TTL)
		if params.StopOnCompletion {
			return nil
		}
	}
	return nil
}

// streamFrom sends the topic's entries from offset to the end of the log. It returns the
// offset after the last entry sent, the read error and the send error. A failed send or a
// departed client cancels the read, and any batch already on its way is drained, so the
// read never blocks on a batch nobody will receive.
func (s server) streamFrom(ctx context.Context, topic common.TopicName, from common.Offset, batchSize uint32, out Ibsen_ReadServer) (common.Offset, error, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logChan := make(chan *[]common.LogEntry)
	var wg sync.WaitGroup
	readDone := make(chan error, 1)
	go func() {
		readDone <- s.manager.Read(manager.ReadParams{
			TopicName: topic,
			From:      from,
			BatchSize: batchSize,
			LogChan:   logChan,
			Wg:        &wg,
			Cancel:    ctx.Done(),
		})
		close(logChan)
	}()

	nextOffset := from
	var sendErr error
	for batch := range logChan {
		if sendErr == nil && len(*batch) > 0 {
			sendErr = out.Send(&OutputEntries{Entries: convert(batch)})
			if sendErr != nil {
				cancel()
			} else {
				nextOffset = common.Offset((*batch)[len(*batch)-1].Offset + 1)
			}
		}
		wg.Done()
	}
	return nextOffset, <-readDone, sendErr
}

func convertTopics(topics []common.TopicName) []string {
	var sTopic []string
	for _, topic := range topics {
		sTopic = append(sTopic, string(topic))
	}
	return sTopic
}

func convert(entries *[]common.LogEntry) []*Entry {
	outEntries := make([]*Entry, len(*entries))
	for i, entry := range *entries {
		outEntries[i] = &Entry{
			Offset:  entry.Offset,
			Content: entry.Entry,
		}
	}
	return outEntries
}

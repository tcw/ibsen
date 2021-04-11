package grpcApi

import (
	"context"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/index"
	"github.com/tcw/ibsen/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/testdata"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type server struct {
	logStorage   storage.LogStorage
	ibsenIndexer index.IbsenIndex
}

type IbsenGrpcServer struct {
	Host         string
	Port         uint16
	CertFile     string
	KeyFile      string
	UseTls       bool
	IbsenServer  *grpc.Server
	Storage      storage.LogStorage
	IbsenIndexer index.IbsenIndex
}

func NewIbsenGrpcServer(storage storage.LogStorage, indexer index.IbsenIndex) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		Host:         "0.0.0.0",
		Port:         50001,
		CertFile:     "",
		KeyFile:      "",
		UseTls:       false,
		Storage:      storage,
		IbsenIndexer: indexer,
	}
}

func (igs *IbsenGrpcServer) StartGRPC(listener net.Listener) error {
	var opts []grpc.ServerOption
	opts = []grpc.ServerOption{
		grpc.ConnectionTimeout(time.Second * 30),
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
		logStorage:   igs.Storage,
		ibsenIndexer: igs.IbsenIndexer,
	})
	return grpcServer.Serve(listener)
}

var _ IbsenServer = &server{}

func (s server) Create(ctx context.Context, topic *Topic) (*CreateStatus, error) {
	create, err := s.logStorage.Create(topic.Name)
	if err != nil {
		log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
		return nil, status.Errorf(codes.Internal, "Failed while creating topic %s", topic.Name)
	}
	err = s.ibsenIndexer.CreateIndex(topic.Name)
	if err != nil {
		log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
		return nil, status.Errorf(codes.Internal, "Failed while creating index for topic %s", topic.Name)
	}
	return &CreateStatus{
		Created: create,
	}, err
}

func (s server) Drop(ctx context.Context, topic *Topic) (*DropStatus, error) {
	dropped, err := s.logStorage.Drop(topic.Name)
	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return nil, status.Errorf(codes.Internal, "Failed while dropping topic %s", topic.Name)
	}
	err = s.ibsenIndexer.DropIndex(topic.Name)
	if err != nil {
		log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
		return nil, status.Errorf(codes.Internal, "Failed while dropping index for topic %s", topic.Name)
	}
	return &DropStatus{
		Dropped: dropped,
	}, err
}

func (s server) Write(ctx context.Context, entries *InputEntries) (*WriteStatus, error) {
	start := time.Now()
	n, err := s.logStorage.WriteBatch(&storage.TopicBatchMessage{
		Topic:   entries.Topic,
		Message: entries.Entries,
	})
	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return nil, status.Error(codes.Unknown, "Error writing batch")
	}

	stop := time.Now()
	timeElapsed := stop.Sub(start)
	return &WriteStatus{
		Wrote:    int64(n),
		TimeNano: timeElapsed.Nanoseconds(),
	}, nil
}

func (s server) WriteStream(inStream Ibsen_WriteStreamServer) error {
	start := time.Now()
	var sum int32
	var entriesWritten int64

	for {
		in, err := inStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			err = errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return status.Error(codes.Unknown, "Error receiving writing streaming batch")
		}
		written, err := s.logStorage.WriteBatch(&storage.TopicBatchMessage{
			Topic:   in.Topic,
			Message: in.Entries,
		})
		if err != nil {
			err = errore.WrapWithContext(err)
			log.Println(errore.SprintTrace(err))
			return status.Error(codes.Unknown, "Error writing streaming batch")
		}
		sum = sum + 1
		entriesWritten = entriesWritten + int64(written)
	}
	stop := time.Now()
	timeElapsed := stop.Sub(start)

	err := inStream.Send(&WriteStatus{
		Wrote:    entriesWritten,
		TimeNano: timeElapsed.Nanoseconds(),
	})
	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return status.Error(codes.Unknown, "Error sending status from writing streaming batch")
	}
	return nil
}

func (s server) Read(readParams *ReadParams, outStream Ibsen_ReadServer) error {
	logChan := make(chan *storage.LogEntryBatch)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, outStream)
	offset, err := s.ibsenIndexer.GetClosestByteOffset(readParams.Topic, commons.Offset(readParams.Offset))
	if err != nil {
		log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
		offset = commons.IndexedOffset{}
	}
	err = s.logStorage.ReadBatch(storage.ReadBatchParam{
		LogChan:      logChan,
		Wg:           &wg,
		Topic:        readParams.Topic,
		BatchSize:    int(readParams.BatchSize),
		Offset:       readParams.Offset,
		IbsenIndexer: offset,
	})

	if err != nil {
		log.Println(errore.SprintTrace(errore.WrapWithContext(err)))
		return status.Error(codes.Unknown, "Error reading streaming")
	}
	wg.Wait()
	return nil
}

func (s server) ReadStream(readParams *ReadParams, outStream Ibsen_ReadStreamServer) error {
	logChan := make(chan *storage.LogEntryBatch)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, outStream)

	err := s.logStorage.ReadStreamingBatch(storage.ReadBatchParam{
		LogChan:   logChan,
		Wg:        &wg,
		Topic:     readParams.Topic,
		BatchSize: int(readParams.BatchSize),
		Offset:    readParams.Offset,
	})

	if err != nil {
		err = errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return status.Error(codes.Unknown, "Error reading streaming")
	}
	wg.Wait()
	return nil
}

func (s server) Status(context.Context, *Empty) (*TopicsStatus, error) {
	logStatus := s.logStorage.Status()
	statuses := make([]*TopicStatus, 0)
	for _, message := range logStatus {
		statuses = append(statuses, &TopicStatus{
			Topic:        message.Topic,
			Blocks:       int64(message.Blocks),
			Offset:       message.Offset,
			MaxBlockSize: message.MaxBlockSize,
			Path:         message.Path,
		})
	}
	return &TopicsStatus{
		TopicStatus: statuses,
	}, nil
}

func (s server) Close() {
	s.logStorage.Close()
}

func sendBatchMessage(logChan chan *storage.LogEntryBatch, wg *sync.WaitGroup, outStream Ibsen_ReadServer) {
	for {
		entryBatch := <-logChan
		if entryBatch.Size() == 0 {
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

func convert(entryBatch *storage.LogEntryBatch) []*Entry {
	entries := entryBatch.Entries
	outEntries := make([]*Entry, len(entries))
	for i, entry := range entries {
		outEntries[i] = &Entry{
			Offset:  entry.Offset,
			Content: entry.Entry,
		}
	}
	return outEntries
}

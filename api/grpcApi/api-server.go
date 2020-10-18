package grpcApi

import (
	"context"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type server struct {
	logStorage logStorage.LogStorage
}

type IbsenGrpcServer struct {
	Port        uint16
	CertFile    string
	KeyFile     string
	UseTls      bool
	IbsenServer *grpc.Server
	Storage     logStorage.LogStorage
}

func NewIbsenGrpcServer(storage logStorage.LogStorage) *IbsenGrpcServer {
	return &IbsenGrpcServer{
		Port:     50001,
		CertFile: "",
		KeyFile:  "",
		UseTls:   false,
		Storage:  storage,
	}
}

func (igs *IbsenGrpcServer) StartGRPC() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", igs.Port))
	if err != nil {
		return err
	}
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
			return err
		}
	}
	grpcServer := grpc.NewServer(opts...)

	igs.IbsenServer = grpcServer

	RegisterIbsenServer(grpcServer, &server{
		logStorage: igs.Storage,
	})

	return grpcServer.Serve(lis)
}

var _ IbsenServer = &server{}

func (s server) Create(ctx context.Context, topic *Topic) (*CreateStatus, error) {
	create, err := s.logStorage.Create(topic.Name)
	return &CreateStatus{
		Created: create,
	}, err
}

func (s server) Drop(ctx context.Context, topic *Topic) (*DropStatus, error) {
	dropped, err := s.logStorage.Drop(topic.Name)
	return &DropStatus{
		Dropped: dropped,
	}, err
}

func (s server) Write(ctx context.Context, entries *InputEntries) (*WriteStatus, error) {
	start := time.Now()
	n, err := s.logStorage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic:   entries.TopicName,
		Message: &entries.Entries,
	})
	if err != nil {
		return nil, err
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
			log.Printf("Failed reading input stream, error: %s", err)
			return err
		}
		written, err := s.logStorage.WriteBatch(&logStorage.TopicBatchMessage{
			Topic:   in.TopicName,
			Message: &in.Entries,
		})
		sum = sum + 1
		entriesWritten = entriesWritten + int64(written)
		if err != nil {
			log.Printf("Failed writing input stream after %d entries, error: %s", entriesWritten, err)
			return err
		}
	}
	stop := time.Now()
	timeElapsed := stop.Sub(start)

	err := inStream.Send(&WriteStatus{
		Wrote:    entriesWritten,
		TimeNano: timeElapsed.Nanoseconds(),
	})
	if err != nil {
		log.Println(err)
	}
	return nil
}

func (s server) Read(readParams *ReadParams, outStream Ibsen_ReadServer) error {
	logChan := make(chan logStorage.LogEntryBatch)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, outStream)

	var err error
	if readParams.Offset == 0 {
		err = s.logStorage.ReadBatchFromBeginning(logChan, &wg, readParams.TopicName, int(readParams.BatchSize))
	} else {
		err = s.logStorage.ReadBatchFromOffsetNotIncluding(logChan, &wg, readParams.TopicName, readParams.Offset, int(readParams.BatchSize))
	}
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s server) Status(context.Context, *Empty) (*TopicsStatus, error) {
	panic("implement me")
}

func (s server) Close() {
	s.logStorage.Close()
}

func sendBatchMessage(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, outStream Ibsen_ReadServer) {
	for {
		entry := <-logChan
		if entry.Size() == 0 {
			break
		}
		err := outStream.Send(&OutputEntries{
			Offset:  uint64(entry.Offset()),
			Entries: entry.ToArray(),
		})
		if err != nil {
			log.Println(err)
		}
		wg.Done()
	}
}

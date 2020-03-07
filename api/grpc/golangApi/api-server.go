package golangApi

import (
	"context"
	"errors"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"io"
	"log"
	"net"
	"sync"
)

type server struct {
	logStorage ext4.LogStorage
}

type IbsenGrpcServer struct {
	Port            uint16
	CertFile        string
	KeyFile         string
	UseTls          bool
	StorageRootPath string
	MaxBlockSize    int64
	IbsenServer     *grpc.Server
	Storage         *ext4.LogStorage
}

func NewIbsenGrpcServer() *IbsenGrpcServer {
	return &IbsenGrpcServer{
		Port:            50001,
		CertFile:        "",
		KeyFile:         "",
		UseTls:          false,
		StorageRootPath: "",
		MaxBlockSize:    1024 * 1024 * 10,
	}
}

func (igs *IbsenGrpcServer) ValidateConfig() []error {
	var configErrors []error
	if igs.StorageRootPath == "" {
		err := errors.New("missing storage root path")
		configErrors = append(configErrors, err)
	}
	return configErrors
}

func (igs *IbsenGrpcServer) StartGRPC() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", igs.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if igs.UseTls {
		absCert := testdata.Path(igs.CertFile)
		absKey := testdata.Path(igs.KeyFile)
		creds, err := credentials.NewServerTLSFromFile(absCert, absKey)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)

	storage, err := ext4.NewLogStorage(igs.StorageRootPath, igs.MaxBlockSize)
	if err != nil {
		grpcServer.GracefulStop()
		return err
	}
	igs.IbsenServer = grpcServer
	igs.Storage = storage
	RegisterIbsenServer(grpcServer, &server{
		logStorage: *storage,
	})

	return grpcServer.Serve(lis)
}

var _ IbsenServer = &server{}

func (s server) Create(ctx context.Context, topic *Topic) (*TopicStatus, error) {
	create, err := s.logStorage.Create(topic.Name)
	return &TopicStatus{
		Created: create,
	}, err
}

func (s server) Drop(context.Context, *Topic) (*TopicStatus, error) {
	panic("implement me")
}

func (s server) Write(context.Context, *TopicMessage) (*Status, error) {
	panic("implement me")
}

func (s server) WriteBatch(ctx context.Context, tbm *TopicBatchMessage) (*Status, error) {

	n, err := s.logStorage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic:   tbm.TopicName,
		Message: &tbm.MessagePayload,
	})
	if err != nil {
		return nil, err
	}

	return &Status{Entries: int32(n)}, nil
}

func (s server) WriteStream(inStream Ibsen_WriteStreamServer) error {
	var sum int
	for {
		in, err := inStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		sum, err = s.logStorage.Write(&logStorage.TopicMessage{
			Topic:   in.TopicName,
			Message: &in.MessagePayload,
		})
		if err != nil {
			return err
		}
	}
	return inStream.SendAndClose(&Status{Entries: int32(sum)})
}

func (s server) ReadFromBeginning(topic *Topic, outStream Ibsen_ReadFromBeginningServer) error {
	logChan := make(chan *logStorage.LogEntry)
	var wg sync.WaitGroup
	go sendMessage(logChan, &wg, outStream)
	err := s.logStorage.ReadFromBeginning(logChan, &wg, topic.Name)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func sendMessage(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, outStream Ibsen_ReadFromBeginningServer) {
	for {
		entry := <-logChan
		err := outStream.Send(&Entry{
			Offset:  entry.Offset,
			Payload: *entry.Entry,
		})
		wg.Done()
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func (s server) ReadFromOffset(*TopicOffset, Ibsen_ReadFromOffsetServer) error {
	panic("implement me")
}

func (s server) ListTopics(*Empty, Ibsen_ListTopicsServer) error {
	panic("implement me")
}

func (s server) ListTopicsWithOffset(*Empty, Ibsen_ListTopicsWithOffsetServer) error {
	panic("implement me")
}

func (s server) Close() {
	s.logStorage.Close()
}

package golangApi

import (
	"context"
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

type IbsenConfig struct {
	port            uint16
	certFile        string
	keyFile         string
	useTls          bool
	storageRootPath string
	maxBlockSize    int64
}

func NewIbsenConfig() *IbsenConfig {
	return &IbsenConfig{
		port:            50001,
		certFile:        "",
		keyFile:         "",
		useTls:          false,
		storageRootPath: "",
		maxBlockSize:    1024 * 1024 * 10,
	}
}

func (config *IbsenConfig) ValidateConfig() {
	if config.storageRootPath == "" {

	}
}

//Todo: should it return true if topic exists?
func (s server) Create(ctx context.Context, topic *Topic) (*TopicStatus, error) {
	create, err := s.logStorage.Create(logStorage.Topic(topic.Name))
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
		sum, err = s.logStorage.Write(logStorage.Topic(in.TopicName), in.MessagePayload)
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
	err := s.logStorage.ReadFromBeginning(logChan, &wg, logStorage.Topic(topic.Name))
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
			Offset:  uint64(entry.Offset),
			Payload: entry.Entry,
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

var _ IbsenServer = &server{}

func StartGRPC(port uint16, certFile string, keyFile string, useTls bool, rootPath string) (*grpc.Server, *ext4.LogStorage, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if useTls {
		if certFile == "" {
			certFile = testdata.Path("key.pem")
		}
		if keyFile == "" {
			keyFile = testdata.Path("cert.key")
		}
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)

	storage, err := ext4.NewLogStorage(rootPath, 1024*1024*10)
	if err != nil {
		grpcServer.GracefulStop()
		return nil, nil, err
	}
	RegisterIbsenServer(grpcServer, &server{
		logStorage: storage,
	})

	err = grpcServer.Serve(lis)
	return grpcServer, &storage, err
}

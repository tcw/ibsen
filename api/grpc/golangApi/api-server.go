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
)

type server struct {
	logStorage ext4.LogStorage
}

func (s server) Create(context.Context, *Topic) (*Empty, error) {
	panic("implement me")
}

func (s server) Drop(context.Context, *Topic) (*Empty, error) {
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

func (s server) ReadFromBeginning(*Topic, Ibsen_ReadFromBeginningServer) error {
	panic("implement me")
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

var _ IbsenServer = &server{}

func StartGRPC(port uint16, certFile string, keyFile string, useTls bool, rootPath string) error {
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
		return err
	}
	RegisterIbsenServer(grpcServer, &server{
		logStorage: storage,
	})
	return grpcServer.Serve(lis)
}

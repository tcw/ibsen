package grpcApi

import (
	"context"
	"dostoevsky/api/golang"
	"dostoevsky/logSequencer"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"io"
	"log"
	"net"
)

type server struct {
	rootPath     string
	topicWriters map[string]*logSequencer.TopicWrite
}

func createWriters(rootPath string, maxBlockSize int64) (map[string]*logSequencer.TopicWrite, error) {
	topics, err := logSequencer.ListTopics(rootPath)
	log.Println(topics)
	writers := make(map[string]*logSequencer.TopicWrite)
	if err != nil {
		return nil, err
	}
	for _, v := range topics {
		log.Println(v)
		writers[v] = logSequencer.NewTopicWrite(rootPath, v, maxBlockSize)
	}
	return writers, nil
}

func (s *server) Drop(context.Context, *grpcApi.Topic) (*grpcApi.Empty, error) {
	panic("implement me")
}

func (s *server) Write(context.Context, *grpcApi.TopicMessage) (*grpcApi.Status, error) {
	panic("implement me")
}

func (s *server) WriteStream(inStream grpcApi.Dostoevsky_WriteStreamServer) error {
	var sum uint64
	for {
		in, err := inStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		s.topicWriters[in.TopicName].WriteToTopic(in.MessagePayload)
		sum = sum + 1
	}
	return inStream.SendAndClose(&grpcApi.Status{Entries: sum})
}

func (s *server) ReadFromBeginning(*grpcApi.Topic, grpcApi.Dostoevsky_ReadFromBeginningServer) error {
	panic("implement me")
}

func (s *server) ReadFromOffset(*grpcApi.TopicOffset, grpcApi.Dostoevsky_ReadFromOffsetServer) error {
	panic("implement me")
}

func (s *server) ListTopics(*grpcApi.Empty, grpcApi.Dostoevsky_ListTopicsServer) error {
	panic("implement me")
}

func (s *server) ListTopicsWithOffset(*grpcApi.Empty, grpcApi.Dostoevsky_ListTopicsWithOffsetServer) error {
	panic("implement me")
}

func (s *server) WriteToLog(ctx context.Context, in *grpcApi.TopicMessage) (*grpcApi.Status, error) {
	log.Printf("Received: %s", in.TopicName)
	return &grpcApi.Status{Entries: 1}, nil
}

func (s *server) Create(ctx context.Context, in *grpcApi.Topic) (*grpcApi.Empty, error) {
	err := logSequencer.CreateNewTopic(s.rootPath, in.Name)
	return &grpcApi.Empty{}, err
}

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

	writers, err := createWriters(rootPath, 1024*1024*100)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("created server on path", rootPath)
	grpcApi.RegisterDostoevskyServer(grpcServer, &server{
		rootPath:     rootPath,
		topicWriters: writers,
	})
	return grpcServer.Serve(lis)
}

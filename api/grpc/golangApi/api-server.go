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

type IbsenGrpcServer struct {
	Port        uint16
	CertFile    string
	KeyFile     string
	UseTls      bool
	IbsenServer *grpc.Server
	Storage     *ext4.LogStorage
}

func NewIbsenGrpcServer(storage *ext4.LogStorage) *IbsenGrpcServer {
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
	if igs.UseTls {
		absCert := testdata.Path(igs.CertFile)
		absKey := testdata.Path(igs.KeyFile)
		creds, err := credentials.NewServerTLSFromFile(absCert, absKey)
		if err != nil {
			return err
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)

	igs.IbsenServer = grpcServer

	RegisterIbsenServer(grpcServer, &server{
		logStorage: *igs.Storage,
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

func (s server) Drop(ctx context.Context, topic *Topic) (*TopicStatus, error) {
	create, err := s.logStorage.Drop(topic.Name)
	return &TopicStatus{
		Created: create,
	}, err
}

func (s server) Write(ctx context.Context, topicMessage *TopicMessage) (*Status, error) {
	_, err := s.logStorage.Write(&logStorage.TopicMessage{
		Topic:   topicMessage.TopicName,
		Message: topicMessage.MessagePayload,
	})
	if err != nil {
		return nil, err
	}
	return &Status{ //Todo return real offset
		Entries: 1,
		Current: &Offset{
			Id: 0,
		},
	}, nil
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
			Message: in.MessagePayload,
		})
		if err != nil {
			return err
		}
	}
	return inStream.SendAndClose(&Status{Entries: int32(sum)})
}

func (s server) ReadFromBeginning(topic *Topic, outStream Ibsen_ReadFromBeginningServer) error {
	logChan := make(chan logStorage.LogEntry)
	var wg sync.WaitGroup
	go sendMessage(logChan, &wg, outStream)
	err := s.logStorage.ReadFromBeginning(logChan, &wg, topic.Name)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s server) ReadFromOffset(topicOffset *TopicOffset, outStream Ibsen_ReadFromOffsetServer) error {
	logChan := make(chan logStorage.LogEntry)
	var wg sync.WaitGroup
	go sendMessage(logChan, &wg, outStream)
	err := s.logStorage.ReadFromNotIncluding(logChan, &wg, topicOffset.TopicName, topicOffset.Offset)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s server) ReadBatchFromBeginning(topic *TopicBatch, outStream Ibsen_ReadBatchFromBeginningServer) error {
	logChan := make(chan logStorage.LogEntryBatch)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, outStream)
	err := s.logStorage.ReadBatchFromBeginning(logChan, &wg, topic.Name, int(topic.BatchSize))
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s server) ReadBatchFromOffset(topicBatchOffest *TopicBatchOffset, outStream Ibsen_ReadBatchFromOffsetServer) error {
	logChan := make(chan logStorage.LogEntryBatch)
	var wg sync.WaitGroup
	go sendBatchMessage(logChan, &wg, outStream)
	err := s.logStorage.ReadBatchFromOffsetNotIncluding(logChan, &wg, topicBatchOffest.TopicName, topicBatchOffest.Offset, int(topicBatchOffest.BatchSize))
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s server) ListTopics(context.Context, *Empty) (*Topics, error) {
	topics, err := s.logStorage.ListTopics()
	if err != nil {
		return nil, err
	}
	return &Topics{
		Name: topics,
	}, nil
}

func (s server) ListTopicsWithOffset(context.Context, *Empty) (*TopicOffsets, error) {
	panic("implement me")
}

func (s server) Close() {
	s.logStorage.Close()
}

func sendMessage(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, outStream Ibsen_ReadFromBeginningServer) {
	for {
		entry := <-logChan
		err := outStream.Send(&Entry{
			Offset:  entry.Offset,
			Payload: entry.Entry,
		})
		wg.Done()
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func sendBatchMessage(logChan chan logStorage.LogEntryBatch, wg *sync.WaitGroup, outStream Ibsen_ReadBatchFromBeginningServer) {
	var grpcEntry []*Entry
	for {
		entry := <-logChan
		log.Printf("sendBatchMessage: %d\n", entry.Size())
		entries := entry.Entries
		log.Printf("sendBatchMessage: %d -> %d\n", entries[0].Offset, entries[len(entries)-1].Offset)
		if entries == nil {
			continue
		}
		for _, v := range entries {
			grpcEntry = append(grpcEntry, &Entry{
				Offset:  v.Offset,
				Payload: v.Entry,
			})
		}
		err := outStream.Send(&EntryBatch{
			Entries: grpcEntry,
		})
		if err != nil {
			log.Println(err)
		}
		wg.Done()
		grpcEntry = nil
	}
}

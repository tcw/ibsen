package ibsen

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/clients/go/api/grpcApi"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driver"
)

// logServer serves the real log manager — producer table and all — through this client's
// own generated stubs. The server's gRPC adapter registers the same proto, and one binary
// cannot link both copies, so the test carries the few lines of adapter it needs.
type logServer struct {
	grpcApi.UnimplementedIbsenServer
	log *manager.LogTopicsManager
}

func (s logServer) Write(_ context.Context, in *grpcApi.InputEntries) (*grpcApi.WriteStatus, error) {
	producer, err := domain.ProducerIDFromBytes(in.ProducerId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.log.Write(driver.WriteParams{Topic: domain.TopicName(in.Topic), Entries: &in.Entries,
		Producer: producer, Sequence: in.Sequence})
	if errors.Is(err, domain.ErrSequenceGap) || errors.Is(err, domain.ErrStaleSequence) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}
	return &grpcApi.WriteStatus{Wrote: int64(result.Count), FirstOffset: uint64(result.First), Duplicate: result.Duplicate}, nil
}

func (s logServer) Read(in *grpcApi.ReadParams, out grpc.ServerStreamingServer[grpcApi.OutputEntries]) error {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	done := make(chan error, 1)
	go func() {
		done <- s.log.Read(driver.ReadParams{TopicName: domain.TopicName(in.Topic), From: domain.Offset(in.Offset),
			BatchSize: in.BatchSize, LogChan: logChan, Wg: &wg})
		close(logChan)
	}()
	for batch := range logChan {
		var entries []*grpcApi.Entry
		for _, e := range *batch {
			entries = append(entries, &grpcApi.Entry{Offset: e.Offset, Content: append([]byte(nil), e.Entry...)})
		}
		_ = out.Send(&grpcApi.OutputEntries{Entries: entries})
		wg.Done()
	}
	if err := <-done; err != nil && !errors.Is(err, domain.NoEntriesFound) {
		return err
	}
	return nil
}

func (s logServer) List(context.Context, *grpcApi.EmptyArgs) (*grpcApi.TopicList, error) {
	var topics []string
	for _, name := range s.log.List() {
		topics = append(topics, string(name))
	}
	return &grpcApi.TopicList{Topics: topics}, nil
}

func startServer(t *testing.T) string {
	t.Helper()
	logManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{Store: memstore.New(), MaxBlockSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	grpcApi.RegisterIbsenServer(server, logServer{log: &logManager})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); logManager.Close() })
	return listener.Addr().String()
}

// loseReplies is a client interceptor that lets a write reach the server and then reports
// it as failed, the first n times: a reply lost on its way back.
func loseReplies(n int32) grpc.DialOption {
	var lost atomic.Int32
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && method == "/Ibsen/write" && lost.Add(1) <= n {
			return status.Error(codes.Unavailable, "the reply was lost")
		}
		return err
	})
}

func readAll(t *testing.T, c *Client, topic string) []string {
	t.Helper()
	var got []string
	err := c.Read(context.Background(), topic, 0, false, false, func(e Entry) error {
		got = append(got, string(e.Content))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestALostReplyIsRetriedAndStoredOnce(t *testing.T) {
	addr := startServer(t)
	client, err := Dial(addr, []Option{WithProducer(), WithRetries(3, time.Millisecond)},
		grpc.WithTransportCredentials(insecure.NewCredentials()), loseReplies(2))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	first, err := client.Write(ctx, "t", [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Duplicate || first.FirstOffset != 0 || first.Count != 2 {
		t.Errorf("after two lost replies the write reported %+v, want the original at 0", first)
	}
	if _, err := client.Write(ctx, "t", [][]byte{[]byte("c")}); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, client, "t"); len(got) != 3 {
		t.Fatalf("the log holds %v, want a, b, c once each", got)
	}
}

func TestWithoutAProducerALostReplyIsNotRetried(t *testing.T) {
	addr := startServer(t)
	client, err := Dial(addr, nil, grpc.WithTransportCredentials(insecure.NewCredentials()), loseReplies(1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write(context.Background(), "t", [][]byte{[]byte("a")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v; without a producer a retry could store it twice, so it is not made", err)
	}
}

func TestARefusedSequenceIsReported(t *testing.T) {
	addr := startServer(t)
	client, err := Dial(addr, []Option{WithProducerID([16]byte{1})})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	if _, err := client.Write(ctx, "t", [][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	client.SetNextSequence("t", 5)
	if _, err := client.Write(ctx, "t", [][]byte{[]byte("b")}); err == nil || status.Code(err) == codes.OK {
		t.Fatal("a gap was accepted")
	}
	if topics, err := client.List(ctx); err != nil || len(topics) != 1 {
		t.Fatalf("list: %v, %v", topics, err)
	}
}

package api

import (
	"context"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/consensus"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"math"
	"math/rand"
	"net"
	"testing"
	"time"
)

const bufSize = 1024 * 1024

func TestIbsenServer_Create_Topic(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	server := createServer()
	go serverStarter(t, lis, &server)
	defer server.ShutdownCleanly()

	ctx := context.Background()
	conn, err := createConnection(ctx, lis)
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	client := grpcApi.NewIbsenClient(conn)

	create, err := client.Create(context.Background(), &grpcApi.Topic{
		Name: "testTopic",
	})
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	if !create.Created {
		t.Fail()
	}
}

func TestIbsenServer_Write_Read_Topic(t *testing.T) {

	lis := bufconn.Listen(bufSize)
	server := createServer()
	go serverStarter(t, lis, &server)
	defer server.ShutdownCleanly()

	ctx := context.Background()
	conn, err := createConnection(ctx, lis)
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	client := grpcApi.NewIbsenClient(conn)

	create, err := client.Create(context.Background(), &grpcApi.Topic{
		Name: "testTopic",
	})
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	if !create.Created {
		t.Fail()
	}
	wrote, err := client.Write(ctx, &grpcApi.InputEntries{
		Topic:   "testTopic",
		Entries: createTestEntries(1000, 100),
	})
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	if wrote.Wrote != 1000 {
		t.Fail()
	}

	read, err := client.Read(ctx, &grpcApi.ReadParams{
		Topic:     "testTopic",
		Offset:    0,
		BatchSize: 10000,
	})
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	recv, err := read.Recv()
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	if len(recv.Entries) != 1000 {
		t.Fail()
	}
}

func serverStarter(t *testing.T, lis *bufconn.Listener, server *IbsenServer) {
	err := server.Start(lis)
	if err != nil {
		t.Error(err)
	}
}

func createConnection(ctx context.Context, lis *bufconn.Listener) (*grpc.ClientConn, error) {

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	return conn, err
}

func createTestEntries(entries int, entriesByteSize int) [][]byte {
	var bytes = make([][]byte, 0)

	for i := 0; i < entries; i++ {
		entry := createTestValues(entriesByteSize)
		bytes = append(bytes, entry)
	}
	return bytes
}

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())

	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

func createServer() IbsenServer {
	afs := &afero.Afero{Fs: afero.NewMemMapFs()}
	return IbsenServer{
		Lock:         consensus.NoFileLock{},
		InMemory:     true,
		Afs:          afs,
		DataPath:     "/tmp/data",
		MaxBlockSize: 10,
		CpuProfile:   "",
		MemProfile:   "",
	}
}
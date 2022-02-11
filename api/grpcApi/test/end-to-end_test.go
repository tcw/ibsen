package test

import (
	"context"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/manager"
	"google.golang.org/grpc"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"testing"
	"time"
)

const rootPath = "/tmp/data"

var afs *afero.Afero
var ibsenServer *grpcApi.IbsenGrpcServer
var target = fmt.Sprintf("%s:%d", "localhost", 50002)

func startGrpcServer() {
	var fs = afero.NewMemMapFs()
	afs = &afero.Afero{Fs: fs}
	err := afs.Mkdir(rootPath, 0600)
	if err != nil {
		log.Fatal(errore.WrapWithContext(err))
	}
	topicsManager, err := manager.NewLogTopicsManager(afs, 15*time.Second, 15*time.Second, rootPath, 1)
	if err != nil {
		log.Fatal(err)
	}
	ibsenServer = grpcApi.NewIbsenGrpcServer(topicsManager)
	lis, err := net.Listen("tcp", target)
	if err != nil {
		log.Fatal(err)
	}
	ibsenServer.StartGRPC(lis)
}

func TestReadWriteVerification(t *testing.T) {
	go startGrpcServer()
	numberOfEntries := 10000
	write(numberOfEntries, 100)
	entries, err := read(0, 1000)
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	if len(entries) != numberOfEntries {
		t.Logf("Actualt entries read %d expected %d", len(entries), numberOfEntries)
		t.Fail()
	}
	ibsenServer.Shutdown()
}

func TestReadWriteWithOffsetVerification(t *testing.T) {
	go startGrpcServer()
	writeEntries := 1000
	write(writeEntries, 100)
	for i := 0; i < writeEntries; i++ {
		offset := uint64(writeEntries - i)
		expected := writeEntries - int(offset)
		entries, err := read(offset, 10)
		if err != nil {
			t.Error(errore.WrapWithContext(err))
		}
		actual := len(entries)
		if actual != expected {
			t.Logf("Actualt entries read %d expected %d", actual, expected)
			t.Fail()
		}
	}
	ibsenServer.Shutdown()
}

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func write(numberOfEntries int, entryByteSize int) {
	client := newIbsenClient(target)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	entries := createInputEntries(numberOfEntries, entryByteSize)
	client.Client.Write(ctx, &entries)
}

func read(offset uint64, batchSize uint32) ([]*grpcApi.Entry, error) {
	client := newIbsenClient(target)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	entryStream, err := client.Client.Read(ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            "test",
		Offset:           offset,
		BatchSize:        batchSize,
	})
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	var entries []*grpcApi.Entry
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return nil, errore.WrapWithContext(err)
		}
		entries = append(entries, in.Entries...)
	}
}

func createInputEntries(numberOfEntries int, entryByteSize int) grpcApi.InputEntries {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createTestValues(entryByteSize))
	}
	return grpcApi.InputEntries{
		Topic:   "test",
		Entries: tmpBytes,
	}
}

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzæøåABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ1234567890")
	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

func newIbsenClient(target string) IbsenClient {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Fatalf(errore.SprintTrace(err))
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute) //Todo: Handle cancel

	return IbsenClient{
		Client: client,
		Ctx:    ctx,
	}
}

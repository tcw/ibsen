package test

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
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
		log.Fatal().Err(err)
	}
	topicsManager, err := manager.NewLogTopicsManager(afs, false, 30*time.Second, 30*time.Second, rootPath, 1)
	if err != nil {
		log.Fatal().Err(err)
	}
	ibsenServer = grpcApi.NewUnsecureIbsenGrpcServer(&topicsManager)
	lis, err := net.Listen("tcp", target)
	if err != nil {
		log.Fatal().Err(err)
	}
	err = ibsenServer.StartGRPC(lis)
	if err != nil {
		log.Fatal().Err(err)
	}
}

func TestTopicList(t *testing.T) {
	go startGrpcServer()
	write("test1", 10, 10)
	write("test2", 10, 10)
	write("test3", 10, 10)
	write("test4", 10, 10)
	write("test5", 10, 10)

	topicList, err := list()
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	assert.Equal(t, 5, len(topicList.GetTopics()), "should be equal")
	ibsenServer.Shutdown()
}

func TestReadWriteLargeObject(t *testing.T) {
	go startGrpcServer()
	numberOfEntries := 1
	objectBytes, err := writeLarge("test", numberOfEntries, 500_000)
	if err != nil {
		t.Error(err)
	}
	entries, err := read("test", 0, uint32(numberOfEntries))
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	assert.Equal(t, numberOfEntries, len(entries), "should be equal")
	actualObjectSize := len(entries[0].Content)
	assert.Equal(t, actualObjectSize, objectBytes, "should be equal")
	ibsenServer.Shutdown()
}

func TestReadWriteVerification(t *testing.T) {
	go startGrpcServer()
	numberOfEntries := 10000
	write("test", numberOfEntries, 100)
	entries, err := read("test", 0, 1000)
	if err != nil {
		t.Error(errore.WrapWithContext(err))
	}
	assert.Equal(t, numberOfEntries, len(entries), "should be equal")
	ibsenServer.Shutdown()
}

func TestReadWriteWithOffsetVerification(t *testing.T) {
	go startGrpcServer()
	writeEntries := 1000
	write("test", writeEntries, 100)
	for i := 1; i < writeEntries; i++ {
		offset := uint64(writeEntries - i)
		expected := writeEntries - int(offset)
		entries, err := read("test", offset, 10)
		if err != nil {
			t.Error(errore.WrapWithContext(err))
		}
		assert.Equal(t, expected, len(entries), "should be equal")
	}
	ibsenServer.Shutdown()
}

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func list() (*grpcApi.TopicList, error) {
	client, err := newIbsenClient(target)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	return client.Client.List(ctx, &empty.Empty{})
}
func writeLarge(topic string, numberOfEntries int, entryKb int) (int, error) {
	client, err := newIbsenClient(target)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return 0, ctx.Err()
	}
	entries, size := createLargeInputEntries(topic, numberOfEntries, entryKb)
	client.Client.Write(ctx, &entries)
	return size, nil
}

func write(topic string, numberOfEntries int, entryByteSize int) error {
	client, err := newIbsenClient(target)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	entries := createInputEntries(topic, numberOfEntries, entryByteSize)
	_, err = client.Client.Write(ctx, &entries)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func read(topic string, offset uint64, batchSize uint32) ([]*grpcApi.Entry, error) {
	client, err := newIbsenClient(target)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	entryStream, err := client.Client.Read(ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
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

func createInputEntries(topic string, numberOfEntries int, entryByteSize int) grpcApi.InputEntries {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createTestValues(entryByteSize))
	}
	return grpcApi.InputEntries{
		Topic:   topic,
		Entries: tmpBytes,
	}
}

func createLargeInputEntries(topic string, numberOfEntries int, entryKB int) (grpcApi.InputEntries, int) {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createLargeTestValues(entryKB))
	}
	byteSize := 0
	for _, tmpByte := range tmpBytes {
		byteSize = byteSize + len(tmpByte)
	}

	return grpcApi.InputEntries{
		Topic:   topic,
		Entries: tmpBytes,
	}, byteSize
}

func createLargeTestValues(entrySizeKB int) []byte {
	var sbKB strings.Builder
	for i := 0; i < 10; i++ {
		sbKB.WriteString("123abcdefghijklmabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	}
	kbString := sbKB.String()
	var large strings.Builder
	for i := 0; i < entrySizeKB; i++ {
		large.WriteString(kbString)
	}
	return []byte(large.String())
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

func newIbsenClient(target string) (IbsenClient, error) {

	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		log.Fatal().Err(err)
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute)
	if ctx.Err() == context.Canceled {
		return IbsenClient{}, ctx.Err()
	}

	return IbsenClient{
		Client: client,
		Ctx:    ctx,
	}, nil
}

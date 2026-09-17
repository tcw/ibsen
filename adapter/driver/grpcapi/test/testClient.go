package test

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IbsenClient struct {
	Client grpcapi.IbsenClient
	conn   *grpc.ClientConn
}

func createInputEntries(topic string, numberOfEntries int, entryByteSize int) grpcapi.InputEntries {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createTestValues(entryByteSize))
	}
	return grpcapi.InputEntries{
		Topic:   topic,
		Entries: tmpBytes,
	}
}

func createLargeInputEntries(topic string, numberOfEntries int, entryKB int) (grpcapi.InputEntries, int) {
	var tmpBytes = make([][]byte, 0)
	for i := 0; i < numberOfEntries; i++ {
		tmpBytes = append(tmpBytes, createLargeTestValues(entryKB))
	}
	byteSize := 0
	for _, tmpByte := range tmpBytes {
		byteSize = byteSize + len(tmpByte)
	}

	return grpcapi.InputEntries{
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

// newIbsenClient connects to target, waiting up to ten seconds for the server to answer.
// Close the client when done.
func newIbsenClient(target string) (IbsenClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return IbsenClient{}, err
	}
	return IbsenClient{
		Client: grpcapi.NewIbsenClient(conn),
		conn:   conn,
	}, nil
}

func (c IbsenClient) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

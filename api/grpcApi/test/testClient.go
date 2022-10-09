package test

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/api/grpcApi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"math/rand"
	"strings"
	"time"
)

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
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

	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
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

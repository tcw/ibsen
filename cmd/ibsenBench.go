package cmd

import (
	"context"
	"fmt"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/grpc"
	"io"
	"log"
	"math"
	"math/rand"
	"time"
)

type IbsenBench struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func newIbsenBench(target string) IbsenBench {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Fatalf(errore.SprintTrace(err))
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute) //Todo: Handle cancel

	return IbsenBench{
		Client: client,
		Ctx:    ctx,
	}
}

func (b *IbsenBench) Benchmark(topic string, writeEntryByteSize int, WriteEntriesInEachBatch int, writeBatches int, readBatchSize int) (string, error) {
	writeReport, err := b.benchWrite(topic, writeEntryByteSize, WriteEntriesInEachBatch, writeBatches)
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	readReport, err := b.benchRead(topic, uint32(readBatchSize))
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	return fmt.Sprintf("%s\n%s", writeReport, readReport), nil
}

func (b *IbsenBench) benchRead(topic string, batchSize uint32) (string, error) {
	entryStream, err := b.Client.Read(b.Ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           0,
		BatchSize:        batchSize,
	})
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	entriesRead := 0
	start := time.Now()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			used := time.Now().Sub(start)
			return fmt.Sprintf("Read\t%d in %s[batchSize:%d]", entriesRead, used, batchSize), nil
		}
		if err != nil {
			return "", errore.WrapWithContext(err)
		}
		entries := in.Entries
		entriesRead = entriesRead + len(entries)
	}
}

func (b *IbsenBench) benchWrite(topic string, entryByteSize int, entriesInEachBatch int, batches int) (string, error) {
	entriesWritten := 0
	inputEntries := createInputEntries(topic, entriesInEachBatch, entryByteSize)
	start := time.Now()
	for i := 0; i < batches; i++ {
		_, err := b.Client.Write(b.Ctx, &inputEntries)
		if err != nil {
			return "", errore.WrapWithContext(err)
		}
		entriesWritten = entriesWritten + len(inputEntries.Entries)
	}
	used := time.Now().Sub(start)
	return fmt.Sprintf("Wrote\t%d in %s\t[batchSize:%d, batches:%d, entryByteSize:%d]",
		entriesWritten, used, entriesInEachBatch, batches, entryByteSize), nil
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

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzæøåABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ1234567890")
	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

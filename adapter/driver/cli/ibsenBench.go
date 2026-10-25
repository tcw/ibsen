package cli

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IbsenBench struct {
	Client grpcapi.IbsenClient
	Ctx    context.Context
	cancel context.CancelFunc
	conn   *grpc.ClientConn
}

func newIbsenBench(target string) (IbsenBench, error) {
	connectCtx, cancelConnect := context.WithTimeout(context.Background(), connectTimeout)
	defer cancelConnect()
	conn, err := grpcapi.DialContext(connectCtx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return IbsenBench{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute)
	return IbsenBench{
		Client: grpcapi.NewIbsenClient(conn),
		Ctx:    ctx,
		cancel: cancel,
		conn:   conn,
	}, nil
}

// Close cancels the context used for calls and closes the connection.
func (b *IbsenBench) Close() {
	b.cancel()
	_ = b.conn.Close()
}

func (b *IbsenBench) BenchmarkConcurrent(
	topic string,
	writeEntryByteSize int,
	writeEntriesInEachBatch int,
	writeBatches int,
	readBatchSize int,
	concurrency int) (string, error) {

	var wgWrite sync.WaitGroup
	wgWrite.Add(concurrency)
	topicPostfixCounter := 1
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		topicWithPostfix := fmt.Sprintf("%s_%d", topic, topicPostfixCounter)
		go b.benchWriteWaitGroup(topicWithPostfix, writeEntryByteSize, writeEntriesInEachBatch, writeBatches, &wgWrite)
	}
	wgWrite.Wait()
	writeTime := time.Now().Sub(start)
	var wgRead sync.WaitGroup
	wgRead.Add(concurrency)
	topicPostfixCounter = 1
	start = time.Now()
	for i := 0; i < concurrency; i++ {
		topicWithPostfix := fmt.Sprintf("%s_%d", topic, topicPostfixCounter)
		go b.benchReadWaitGroup(topicWithPostfix, uint32(readBatchSize), &wgRead)
	}
	wgRead.Wait()
	readTime := time.Now().Sub(start)
	totalWritten := writeEntriesInEachBatch * writeBatches * concurrency
	return fmt.Sprintf("wrote\t%d in %s\nread\t%d in %s", totalWritten, writeTime, totalWritten, readTime), nil
}

func (b *IbsenBench) Benchmark(
	topic string,
	writeEntryByteSize int,
	writeEntriesInEachBatch int,
	writeBatches int,
	readBatchSize int) (string, error) {

	writeReport, err := b.benchWrite(topic, writeEntryByteSize, writeEntriesInEachBatch, writeBatches)
	if err != nil {
		return "", err
	}
	readReport, err := b.benchRead(topic, uint32(readBatchSize))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n%s", writeReport, readReport), nil
}

func (b *IbsenBench) benchReadWaitGroup(topic string, batchSize uint32, wg *sync.WaitGroup) {
	_, err := b.benchRead(topic, batchSize)
	if err != nil {
		log.Fatal().Err(err).Msg("concurrent bench read failed")
	}
	wg.Done()
}

func (b *IbsenBench) benchRead(topic string, batchSize uint32) (string, error) {
	entryStream, err := b.Client.Read(b.Ctx, &grpcapi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           0,
		BatchSize:        batchSize,
	})
	if err != nil {
		return "", err
	}
	entriesRead := 0
	start := time.Now()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			used := time.Now().Sub(start)
			return fmt.Sprintf("Read\t%d in %s [batchSize:%d]", entriesRead, used, batchSize), nil
		}
		if err != nil {
			return "", err
		}
		entries := in.Entries
		entriesRead = entriesRead + len(entries)
	}
}

func (b *IbsenBench) benchWriteWaitGroup(topic string, entryByteSize int, entriesInEachBatch int, batches int, wg *sync.WaitGroup) {
	_, err := b.benchWrite(topic, entryByteSize, entriesInEachBatch, batches)
	if err != nil {
		log.Fatal().Err(err).Msg("concurrent bench write failed")
	}
	wg.Done()
}

func (b *IbsenBench) benchWrite(topic string, entryByteSize int, entriesInEachBatch int, batches int) (string, error) {
	entriesWritten := 0
	inputEntries := createInputEntries(topic, entriesInEachBatch, entryByteSize)
	start := time.Now()
	for i := 0; i < batches; i++ {
		_, err := b.Client.Write(b.Ctx, &inputEntries)
		if err != nil {
			return "", err
		}
		entriesWritten = entriesWritten + len(inputEntries.Entries)
	}
	used := time.Now().Sub(start)
	return fmt.Sprintf("Wrote\t%d in %s\t[batchSize:%d, batches:%d, entryByteSize:%d]",
		entriesWritten, used, entriesInEachBatch, batches, entryByteSize), nil
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

func createTestValues(entrySizeBytes int) []byte {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzæøåABCDEFGHIJKLMNOPQRSTUVWXYZÆØÅ1234567890")
	b := make([]rune, entrySizeBytes)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/grpc"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
)

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func Start(target string) IbsenClient {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Fatalf(errore.SprintTrace(err))
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(30)*time.Second) //Todo: Handle cancel

	return IbsenClient{
		Client: client,
		Ctx:    ctx,
	}
}

func (ic *IbsenClient) CreateTopic(topic string) bool {

	create, err := ic.Client.Create(ic.Ctx, &grpcApi.Topic{
		Name: topic,
	})
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
	}
	return create.Created
}

func (ic *IbsenClient) Status() {
	status := ic.Status
	prettyJSON, err := json.MarshalIndent(status, "", "    ")
	if err != nil {
		log.Print(errore.SprintTrace(err))
	}
	stdout := os.Stdout
	writer := bufio.NewWriter(stdout)
	_, err = writer.Write(prettyJSON)
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
	}
}

func (ic *IbsenClient) ReadTopic(topic string, offset uint64, batchSize uint32) {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		Topic:     topic,
		Offset:    offset,
		BatchSize: batchSize,
	})
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return
	}
	stdout := os.Stdout
	writer := bufio.NewWriter(stdout)
	defer stdout.Close()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			err := writer.Flush()
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			return
		}
		entries := in.Entries
		for _, entry := range entries {
			line := fmt.Sprintf("%d\t%s\n", entry.Offset, string(entry.Content))
			_, err = writer.Write([]byte(line))
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Println(errore.SprintTrace(err))
				return
			}
		}
	}
}

func (ic *IbsenClient) ReadSteamingTopic(topic string, offset uint64, batchSize uint32) {
	entryStream, err := ic.Client.ReadStream(ic.Ctx, &grpcApi.ReadParams{
		Topic:     topic,
		Offset:    offset,
		BatchSize: batchSize,
	})
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return
	}
	stdout := os.Stdout
	writer := bufio.NewWriter(stdout)
	defer stdout.Close()
	for {
		in, err := entryStream.Recv()
		entries := in.Entries
		for _, entry := range entries {
			line := fmt.Sprintf("%d\t%s\n", entry.Offset, string(entry.Content))
			_, err = writer.Write([]byte(line))
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Println(errore.SprintTrace(err))
				return
			}
		}
		writer.Flush()
	}
}

func (ic *IbsenClient) WriteTopic(topic string) {
	r, err := ic.Client.WriteStream(ic.Ctx)
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Fatalf(errore.SprintTrace(err))
	}

	stdin := os.Stdin
	defer stdin.Close()
	scanner := bufio.NewScanner(stdin)
	const maxCapacity = 1024 * 1024 * 10
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	var tmpBytes = make([][]byte, 0)
	var limitCounter = 0
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			err := errore.WrapWithContext(err)
			fmt.Println(errore.SprintTrace(err))
		}
		text := scanner.Text()
		if text == "" {
			continue
		}
		tmpBytes = append(tmpBytes, []byte(text))
		limitCounter = limitCounter + 1
		if limitCounter == 2 {
			mes := grpcApi.InputEntries{
				Topic:   topic,
				Entries: tmpBytes,
			}
			err = r.Send(&mes)
			if err != nil {
				err := errore.WrapWithContext(err)
				fmt.Println(errore.SprintTrace(err))
			}
			limitCounter = 0
			tmpBytes = make([][]byte, 0)
		}
	}
	if limitCounter > 0 {
		mes := grpcApi.InputEntries{
			Topic:   topic,
			Entries: tmpBytes,
		}
		err = r.Send(&mes)
		if err != nil {
			err := errore.WrapWithContext(err)
			fmt.Println(errore.SprintTrace(err))
		}
	}
	err = r.CloseSend()
	if err != nil {
		err := errore.WrapWithContext(err)
		fmt.Println(errore.SprintTrace(err))
	}

	recv, err := r.Recv()
	if err != nil {
		err := errore.WrapWithContext(err)
		fmt.Println(errore.SprintTrace(err))
	}
	milliWithFracion := float64(recv.TimeNano) / float64(time.Millisecond)
	fmt.Printf("Wrote %d entriesInBatch in %f ms\n", recv.Wrote, milliWithFracion)
}

func (ic *IbsenClient) BenchWrite(topic string, entryByteSize int, entriesInBatch int, batches int) {
	r, err := ic.Client.WriteStream(ic.Ctx)
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Fatalf(errore.SprintTrace(err))
	}

	var bytes = make([][]byte, 0)

	for i := 0; i < entriesInBatch; i++ {
		entry := createTestValues(entryByteSize)
		bytes = append(bytes, entry)
	}

	for i := 0; i < batches; i++ {
		err = r.Send(&grpcApi.InputEntries{
			Topic:   topic,
			Entries: bytes,
		})
	}

	err = r.CloseSend()
	if err != nil {
		err := errore.WrapWithContext(err)
		fmt.Println(errore.SprintTrace(err))
		return
	}
	recv, err := r.Recv()
	if err != nil {
		err := errore.WrapWithContext(err)
		fmt.Println(errore.SprintTrace(err))
	}

	timeUsed := float64(recv.TimeNano)
	log.Println(timeUsed)
	milliWithFracion := timeUsed / float64(time.Millisecond)
	fmt.Printf("Entries written:\t%d\nEntry size:\t\t%d Bytes\nBatches:\t\t%d\nEntires each batch:\t%d\nTime:\t\t\t%f ms\n", recv.Wrote, entryByteSize, batches, entriesInBatch, milliWithFracion)
	fmt.Printf("Used %d nano seconds per entry\n", recv.TimeNano/recv.Wrote)
}

func (ic *IbsenClient) BenchRead(topic string, offset uint64, batchSize uint32) {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		Topic:     topic,
		Offset:    offset,
		BatchSize: batchSize,
	})
	if err != nil {
		err := errore.WrapWithContext(err)
		log.Println(errore.SprintTrace(err))
		return
	}
	start := time.Now()
	stdout := os.Stdout
	writer := bufio.NewWriter(stdout)
	var totalEntriesRead int64 = 0
	defer stdout.Close()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			err := writer.Flush()
			if err != nil {
				err := errore.WrapWithContext(err)
				log.Fatal(errore.SprintTrace(err))
			}
			break
		}
		if in != nil && in.Entries != nil {
			totalEntriesRead = totalEntriesRead + int64(len(in.GetEntries()))
		}
	}
	stop := time.Now()
	elapsedTime := stop.Sub(start)
	milliWithFraction := float64(elapsedTime.Nanoseconds()) / float64(time.Millisecond)
	fmt.Printf("Read entries:\t%d\nBatch size:\t%d\nTime:\t%f ms\n", totalEntriesRead, batchSize, milliWithFraction)
	if totalEntriesRead > 0 {
		fmt.Printf("Used %d nano seconds per entry\n", elapsedTime.Nanoseconds()/totalEntriesRead)
	}
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

package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpcApi"
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
		log.Fatalf("did not connect: %v", err)
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
		log.Println(err)
	}
	return create.Created
}

func (ic *IbsenClient) Status() {
	status := ic.Status
	prettyJSON, err := json.MarshalIndent(status, "", "    ")
	if err != nil {
		log.Print(err)
	}
	stdout := os.Stdout
	writer := bufio.NewWriter(stdout)
	_, err = writer.Write(prettyJSON)
	if err != nil {
		log.Println(err)
	}
}

func (ic *IbsenClient) ReadTopic(topic string, offset uint64, batchSize uint32) {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		TopicName: topic,
		Offset:    offset,
		BatchSize: batchSize,
	})
	if err != nil {
		log.Println(err)
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
				log.Fatal("flush error")
			}
			return
		}
		entries := in.Entries
		for _, entry := range entries {
			line := fmt.Sprintf("%s\n", string(entry))
			_, err = writer.Write([]byte(line))
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (ic *IbsenClient) WriteTopic(topic string) {
	r, err := ic.Client.WriteStream(ic.Ctx)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
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
			fmt.Println(err)
		}
		text := scanner.Text()
		if text == "" {
			continue
		}
		tmpBytes = append(tmpBytes, []byte(text))
		limitCounter = limitCounter + 1
		if limitCounter == 2 {
			mes := grpcApi.InputEntries{
				TopicName: topic,
				Entries:   tmpBytes,
			}
			err = r.Send(&mes)
			if err != nil {
				fmt.Println(err)
			}
			limitCounter = 0
			tmpBytes = make([][]byte, 0)
		}
	}
	if limitCounter > 0 {
		fmt.Println("bytes", tmpBytes)
		mes := grpcApi.InputEntries{
			TopicName: topic,
			Entries:   tmpBytes,
		}
		err = r.Send(&mes)
		if err != nil {
			fmt.Println(err)
		}
	}
	err = r.CloseSend()
	if err != nil {
		fmt.Println(err)
	}

	recv, err := r.Recv()
	if err != nil {
		fmt.Println(err)
	}
	milliWithFracion := float64(recv.TimeNano) / float64(time.Millisecond)
	fmt.Printf("Wrote %d entriesInBatch in %f ms\n", recv.Wrote, milliWithFracion)
}

func (ic *IbsenClient) BenchWrite(topic string, entryByteSize int, entriesInBatch int, batches int) {
	r, err := ic.Client.WriteStream(ic.Ctx)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	var bytes = make([][]byte, 0)

	for i := 0; i < entriesInBatch; i++ {
		entry := createTestValues(entryByteSize)
		bytes = append(bytes, entry)
	}

	for i := 0; i < batches; i++ {
		err = r.Send(&grpcApi.InputEntries{
			TopicName: topic,
			Entries:   bytes,
		})
	}

	err = r.CloseSend()
	if err != nil {
		fmt.Println(err)
		return
	}
	recv, err := r.Recv()
	if err != nil {
		fmt.Println(err)
	}
	milliWithFracion := float64(recv.TimeNano) / float64(time.Millisecond)
	fmt.Printf("Entries written:\t%d\nEntry size:\t\t%d Bytes\nBatches:\t\t%d\nEntires each batch:\t%d\nTime:\t\t\t%f ms\n", recv.Wrote, entryByteSize, batches, entriesInBatch, milliWithFracion)
	fmt.Printf("Used %d nano seconds per entry\n", recv.TimeNano/recv.Wrote)
}

func (ic *IbsenClient) BenchRead(topic string, offset uint64, batchSize uint32) {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		TopicName: topic,
		Offset:    offset,
		BatchSize: batchSize,
	})
	if err != nil {
		log.Println(err)
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
				log.Fatal("flush error")
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
	fmt.Printf("Used %d nano seconds per entry\n", elapsedTime.Nanoseconds()/totalEntriesRead)
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

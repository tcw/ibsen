package client

import (
	"bufio"
	"context"
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
		line := fmt.Sprintf("%d", in.Offset)
		_, err = writer.Write([]byte(line))
		if err != nil {
			log.Println(err)
			return
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
	fmt.Printf("wrote %d entries", recv.Wrote)

}

func (ic *IbsenClient) WriteTestDataToTopic(topic string, entryByteSize int, entries int) {
	clientDeadline := time.Now().Add(time.Second * 30)
	ctx, _ := context.WithDeadline(ic.Ctx, clientDeadline)

	r, err := ic.Client.WriteStream(ctx)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	var bytes = make([][]byte, 0)

	for i := 0; i < entries; i++ {
		entry := createTestValues(entryByteSize)
		bytes = append(bytes, entry)
	}

	err = r.Send(&grpcApi.InputEntries{
		TopicName: topic,
		Entries:   bytes,
	})

	err = r.CloseSend()
	if err != nil {
		fmt.Println(err)
		return
	}
	recv, err := r.Recv()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Wrote %d entries", recv.Wrote)
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

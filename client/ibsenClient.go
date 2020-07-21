package client

import (
	"bufio"
	"context"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func Start(target string) IbsenClient {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second) //Todo: Handle cancel

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

func (ic *IbsenClient) ReadTopic(topic string) {
	entryStream, err := ic.Client.ReadFromBeginning(ic.Ctx, &grpcApi.Topic{
		Name: topic,
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
		line := fmt.Sprintf("%d\t%s\n", in.Offset, in.Payload)
		_, err = writer.Write([]byte(line))
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func (ic *IbsenClient) ReadTopicFromNotIncludingOffset(topic string, offset uint64) {
	entryStream, err := ic.Client.ReadFromOffset(ic.Ctx, &grpcApi.TopicOffset{
		TopicName: topic,
		Offset:    offset,
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
		line := fmt.Sprintf("%d\t%s\n", in.Offset, in.Payload)
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
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
			continue
		}
		mes := grpcApi.TopicMessage{
			TopicName:      topic,
			MessagePayload: []byte(text),
		}
		err = r.Send(&mes)
		recv, err := r.Recv() //TODO: do async
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Printf("Confirmed offset %s\n", strconv.FormatUint(recv.Current.Id, 10))
	}
	err = r.CloseSend()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (ic *IbsenClient) WriteTestDataToTopic(topic string, entrySize int) {
	r, err := ic.Client.WriteStream(ic.Ctx)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	entry := createTestValues(entrySize)

	err = r.Send(&grpcApi.TopicMessage{
		TopicName:      topic,
		MessagePayload: entry,
	})

	err = r.CloseSend()
	if err != nil {
		fmt.Println(err)
		return
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

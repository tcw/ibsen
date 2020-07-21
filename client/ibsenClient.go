package client

import (
	"bufio"
	"context"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
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
		if err != nil {
			log.Println(err)
			return
		}
	}
	_, err = r.CloseAndRecv()
	if err != nil {
		log.Println(err)
		return
	}
}

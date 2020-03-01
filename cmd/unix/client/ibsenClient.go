package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"google.golang.org/grpc"
	"log"
	"os"
	"time"
)

var (
	verbose = flag.Bool("v", false, "Verbose")
	topic   = flag.String("t", "", "Topic name")
)

func main() {

	flag.Parse()
	log.Println("creating client...")
	conn, err := grpc.Dial("localhost:50001", grpc.WithInsecure(), grpc.WithBlock())
	log.Println("created client")
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := grpcApi.NewIbsenClient(conn)

	log.Println("created client")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r, err := c.WriteStream(ctx)
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
		log.Println(text)
		bytes, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			fmt.Println(err)
			return
		}
		mes := grpcApi.TopicMessage{
			TopicName:      *topic,
			MessagePayload: bytes,
		}
		err = r.Send(&mes)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	_, err = r.CloseAndRecv()
	if err != nil {
		fmt.Println(err)
		return
	}

}

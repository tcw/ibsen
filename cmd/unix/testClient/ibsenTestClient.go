package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

var (
	verbose         = flag.Bool("v", false, "Verbose")
	numberOfEntries = flag.Int("n", 0, "Number of entries to write to topic")
	useTestTopic    = flag.String("t", "test", "Test topic name ")
	entryTestSize   = flag.Int("b", 100, "Each message will contain n bytes")
	readTopic       = flag.String("r", "", "Read Topic from beginning")
)

func main() {

	// go tool pprof --pdf ibsen cpu.pprof > callgraph.pdf
	// evince callgraph.pdf

	flag.Parse()
	conn, err := grpc.Dial("localhost:50001", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := grpcApi.NewIbsenClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if *numberOfEntries > 0 {
		_, err := c.Create(ctx, &grpcApi.Topic{
			Name: *useTestTopic,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		r, err := c.WriteStream(ctx)
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}

		rand.Seed(time.Now().UnixNano())

		var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

		b := make([]rune, *entryTestSize)
		for i := range b {
			b[i] = letterRunes[rand.Intn(len(letterRunes))]
		}

		mes := grpcApi.TopicMessage{
			TopicName:      *useTestTopic,
			MessagePayload: []byte(string(b)),
		}
		for i := 0; i < *numberOfEntries; i++ {
			err = r.Send(&mes)
			if err != nil {
				fmt.Println("sent ", i, " messages", err)
				return
			}
		}
		_, err = r.CloseAndRecv()
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	if *readTopic != "" {
		entryStream, err := c.ReadFromBeginning(ctx, &grpcApi.Topic{
			Name: *readTopic,
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
					log.Fatal(err)
				}
				return
			}
			payLoadBase64 := base64.StdEncoding.EncodeToString(in.Payload)
			line := fmt.Sprintf("%d\t%s\n", in.Offset, payLoadBase64)
			_, err = writer.Write([]byte(line))
			if err != nil {
				log.Println(err)
				return
			}
		}
	}

}

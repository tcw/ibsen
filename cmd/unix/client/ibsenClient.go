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
	writeTopic      = flag.String("w", "", "Write to Topic")
	readTopic       = flag.String("r", "", "Read from Topic")
	createTopic     = flag.String("c", "", "Create Topic")
	createNTestData = flag.Int("tn", 0, "Number of entries to [test] topic")
	useTestTopic    = flag.String("tt", "test", "Test topic name ")
	entryTestSize   = flag.Int("ts", 100, "Each message will contain n bytes")
)

func main() {

	flag.Parse()
	conn, err := grpc.Dial("localhost:50001", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := grpcApi.NewIbsenClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if *createTopic != "" {
		topic, err := c.Create(ctx, &grpcApi.Topic{
			Name: *createTopic,
		})
		if err != nil {
			fmt.Println(err)
		}
		if *verbose {
			if topic.Created {
				fmt.Printf("Created Topic [%s]", *createTopic)
			} else {
				fmt.Println("Topic", *createTopic, " already exists!")
			}
		}
	}

	if *createNTestData > 0 {
		_, err := c.Create(ctx, &grpcApi.Topic{
			Name: "test",
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
		for i := 0; i < *createNTestData; i++ {
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
				writer.Flush()
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

	if *writeTopic != "" {
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
				TopicName:      *writeTopic,
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
}

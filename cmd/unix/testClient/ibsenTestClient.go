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
	"math"
	"math/rand"
	"os"
	"time"
)

var (
	verbose         = flag.Bool("v", false, "Verbose")
	numberOfEntries = flag.Int("w", 0, "Number of entries to write to topic")
	batches         = flag.Int("b", 0, "Number of entries to write to topic")
	readBatchSize   = flag.Int("bs", 1000, "ReadBatch size")
	useTestTopic    = flag.String("t", "test", "Test topic name ")
	entryTestSize   = flag.Int("es", 100, "Each message will contain n bytes")
	readTopic       = flag.String("r", "", "Read Topic from beginning")
	readBatchTopic  = flag.String("rb", "", "Read Topic from beginning in batches")
)

func main() {

	// go tool pprof --pdf ibsen cpu.pprof > callgraph.pdf
	// evince callgraph.pdf

	flag.Parse()
	conn, err := grpc.Dial("localhost:50001", grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := grpcApi.NewIbsenClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if *batches > 0 {
		_, err := c.Create(ctx, &grpcApi.Topic{
			Name: *useTestTopic,
		})
		if err != nil {
			fmt.Println(err)
			return
		}

		rand.Seed(time.Now().UnixNano())

		var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

		b := make([]rune, *entryTestSize)
		for i := range b {
			b[i] = letterRunes[rand.Intn(len(letterRunes))]
		}

		var bytes [][]byte
		for i := 0; i < *numberOfEntries; i++ {
			bytes = append(bytes, []byte(string(b)))
		}

		mes := grpcApi.TopicBatchMessage{
			TopicName:      *useTestTopic,
			MessagePayload: bytes,
		}
		for i := 0; i < *batches; i++ {
			_, err = c.WriteBatch(ctx, &mes)
			if err != nil {
				fmt.Println("error writing batch")
			}
		}
		return
	}

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

	if *readBatchTopic != "" {
		entryStream, err := c.ReadBatchFromBeginning(ctx, &grpcApi.TopicBatch{
			Name:      *readBatchTopic,
			BatchSize: uint32(*readBatchSize),
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
			if err != nil {
				log.Fatal(err)
			}
			if in == nil {
				continue
			}
			fmt.Println(len(in.Entries))
			//entries := in.Entries
			/*for _, v := range entries {
				payLoadBase64 := base64.StdEncoding.EncodeToString(v.Payload)
				line := fmt.Sprintf("%d\t%s\n", v.Offset, payLoadBase64)
				_, err = writer.Write([]byte(line))
				if err != nil {
					log.Println(err)
					return
				}
			}*/
		}
	}

}

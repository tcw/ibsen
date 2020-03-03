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
	"os"
	"time"
)

var (
	verbose    = flag.Bool("v", false, "Verbose")
	writeTopic = flag.String("w", "", "Write to Topic")
	readTopic  = flag.String("r", "", "Read from Topic")
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

	if *readTopic != "" {
		entryStream, err := c.ReadFromBeginning(ctx, &grpcApi.Topic{
			Name: *readTopic,
		})
		if err != nil {
			log.Println(err)
			return
		}
		stdout := os.Stdout
		defer stdout.Close()
		for {
			in, err := entryStream.Recv()
			if err == io.EOF {
				return
			}
			payLoadBase64 := base64.StdEncoding.EncodeToString(in.Payload)
			bytes := append([]byte(payLoadBase64), []byte("\n")...)
			_, err = stdout.Write(bytes)
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

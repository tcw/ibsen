package cmd

import (
	"bufio"
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/grpc"
	"io"
	"math"
	"os"
	"time"
)

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func newIbsenClient(target string) IbsenClient {
	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		log.Fatal().Err(err)
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute) //Todo: Handle cancel

	return IbsenClient{
		Client: client,
		Ctx:    ctx,
	}
}

func (ic *IbsenClient) Read(topic string, offset uint64, batchSize uint32) error {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        batchSize,
	})
	if err != nil {
		return errore.WrapWithContext(err)
	}

	stdout := os.Stdout
	defer stdout.Close()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		entries := in.Entries
		for _, entry := range entries {
			line := fmt.Sprintf("%d\t%s\n", entry.Offset, string(entry.Content))
			_, err = stdout.Write([]byte(line))
			if err != nil {
				return errore.WrapWithContext(err)
			}
		}
	}
}

func (ic *IbsenClient) Write(topic string, fileName ...string) (string, error) {
	start := time.Now()
	var reader io.Reader

	if len(fileName) == 0 {
		stdin := os.Stdin
		defer stdin.Close()
		reader = stdin
	} else {
		file, err := os.OpenFile(fileName[0], os.O_RDONLY, 0400)
		defer file.Close()
		reader = file
		if err != nil {
			return "", errore.WrapWithContext(err)
		}
	}

	inputScanner := bufio.NewScanner(reader)

	const maxCapacity = 1024 * 1024 * 10
	buf := make([]byte, maxCapacity)
	inputScanner.Buffer(buf, maxCapacity)
	var tmpBytes = make([][]byte, 0)
	var batchSize = 0
	var entriesRead = 0
	var entriesWritten = 0
	for inputScanner.Scan() {
		if err := inputScanner.Err(); err != nil {
			return "", errore.WrapWithContext(err)
		}
		text := inputScanner.Text()
		if text == "" {
			continue
		}
		entriesRead = entriesRead + 1
		tmpBytes = append(tmpBytes, []byte(text))
		batchSize = batchSize + 1
		if batchSize == 1000 {
			mes := grpcApi.InputEntries{
				Topic:   topic,
				Entries: tmpBytes,
			}
			_, err := ic.Client.Write(ic.Ctx, &mes)
			if err != nil {
				return "", errore.WrapWithContext(err)
			}
			entriesWritten = entriesWritten + len(mes.Entries)
			batchSize = 0
			tmpBytes = make([][]byte, 0)
		}
	}
	if len(tmpBytes) > 0 {
		mes := grpcApi.InputEntries{
			Topic:   topic,
			Entries: tmpBytes,
		}
		_, err := ic.Client.Write(ic.Ctx, &mes)
		if err != nil {
			return "", errore.WrapWithContext(err)
		}
		entriesWritten = entriesWritten + len(tmpBytes)
	}
	used := time.Now().Sub(start)
	return fmt.Sprintf("Wrote %d to %s topic in %s\n", entriesWritten, topic, used), nil
}

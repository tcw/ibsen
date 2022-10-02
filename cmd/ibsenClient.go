package cmd

import (
	"bufio"
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/errore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

type IbsenClient struct {
	Client grpcApi.IbsenClient
	Ctx    context.Context
}

func newIbsenClient(target string) (IbsenClient, error) {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		log.Fatal().Err(err)
	}

	client := grpcApi.NewIbsenClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Minute)
	if ctx.Err() == context.Canceled {
		return IbsenClient{}, ctx.Err()
	}

	return IbsenClient{
		Client: client,
		Ctx:    ctx,
	}, nil
}

func (ic *IbsenClient) List() (string, error) {
	list, err := ic.Client.List(ic.Ctx, &grpcApi.EmptyArgs{})
	if err != nil {
		return "", err
	}
	return strings.Join(list.Topics, "\n"), nil
}

func (ic *IbsenClient) Read(topic string, offset uint64, batchSize uint32) error {
	entryStream, err := ic.Client.Read(ic.Ctx, &grpcApi.ReadParams{
		StopOnCompletion: false,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        batchSize,
	})
	if err != nil {
		return err
	}

	stdout := os.Stdout
	defer stdout.Close()
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		entries := in.Entries
		for _, entry := range entries {
			line := fmt.Sprintf("%d\t%s\n", entry.Offset, string(entry.Content))
			_, err = stdout.Write([]byte(line))
			if err != nil {
				return err
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
		reader = file
		if err != nil {
			ioErr := file.Close()
			return "", errore.WrapError(ioErr, err)
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
			return "", err
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
				return "", err
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
			return "", err
		}
		entriesWritten = entriesWritten + len(tmpBytes)
	}
	used := time.Now().Sub(start)
	return fmt.Sprintf("Wrote %d to %s topic in %s\n", entriesWritten, topic, used), nil
}

package test

import (
	"context"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/api/grpcApi"
	"io"
	"testing"
	"time"
)

func TestTopicList(t *testing.T) {
	afs := newMemMapFs()
	go startGrpcServer(afs, "/tmp/data")
	err := write("test1", 10, 10)
	assert.Nil(t, err)
	err = write("test2", 10, 10)
	assert.Nil(t, err)
	err = write("test3", 10, 10)
	assert.Nil(t, err)
	err = write("test4", 10, 10)
	assert.Nil(t, err)
	err = write("test5", 10, 10)
	assert.Nil(t, err)

	topicList, err := list()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, len(topicList.GetTopics()), "should be equal")
	ibsenServer.Shutdown()
}

func newMemMapFs() *afero.Afero {
	var fs = afero.NewMemMapFs()
	return &afero.Afero{Fs: fs}
}

func TestReadWriteLargeObject(t *testing.T) {
	afs := newMemMapFs()
	go startGrpcServer(afs, "/tmp/data")
	numberOfEntries := 1
	objectBytes, err := writeLarge("test", numberOfEntries, 500_000)
	if err != nil {
		t.Error(err)
	}
	entries, err := read("test", 0, uint32(numberOfEntries))
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, numberOfEntries, len(entries), "should be equal")
	actualObjectSize := len(entries[0].Content)
	assert.Equal(t, actualObjectSize, objectBytes, "should be equal")
	ibsenServer.Shutdown()
}

func TestReadWriteVerification(t *testing.T) {
	afs := newMemMapFs()
	go startGrpcServer(afs, "/tmp/data")
	numberOfEntries := 10000
	err := write("test", numberOfEntries, 100)
	assert.Nil(t, err)
	entries, err := read("test", 0, 1000)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, numberOfEntries, len(entries), "should be equal")
	ibsenServer.Shutdown()
}

// Todo: fix
//func TestReadWriteWithOffsetVerification(t *testing.T) {
//	afs := newMemMapFs()
//	go startGrpcServer(afs, "/tmp/data")
//	writeEntries := 1000
//	err := write("test", writeEntries, 100)
//	assert.Nil(t, err)
//	for i := 1; i < writeEntries; i++ {
//		offset := uint64(writeEntries - i)
//		expected := writeEntries - int(offset)
//		entries, err := read("test", offset, 10)
//		if err != nil {
//			t.Error(err)
//		}
//		assert.Equal(t, expected, len(entries), "should be equal")
//	}
//	ibsenServer.Shutdown()
//}

func list() (*grpcApi.TopicList, error) {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	return client.Client.List(ctx, &grpcApi.EmptyArgs{})
}
func writeLarge(topic string, numberOfEntries int, entryKb int) (int, error) {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return 0, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return 0, ctx.Err()
	}
	entries, size := createLargeInputEntries(topic, numberOfEntries, entryKb)
	client.Client.Write(ctx, &entries)
	return size, nil
}

func write(topic string, numberOfEntries int, entryByteSize int) error {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	entries := createInputEntries(topic, numberOfEntries, entryByteSize)
	_, err = client.Client.Write(ctx, &entries)
	if err != nil {
		return err
	}
	return nil
}

func read(topic string, offset uint64, batchSize uint32) ([]*grpcApi.Entry, error) {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if ctx.Err() == context.Canceled {
		return nil, ctx.Err()
	}
	entryStream, err := client.Client.Read(ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        batchSize,
	})
	if err != nil {
		return nil, err
	}
	var entries []*grpcApi.Entry
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, in.Entries...)
	}
}

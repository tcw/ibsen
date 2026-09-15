package test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/api/grpcApi"
	"io"
	"testing"
	"time"
)

func TestTopicList(t *testing.T) {
	startTestServer(t)
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
		t.Fatal(err)
	}
	assert.Equal(t, 5, len(topicList.GetTopics()), "should be equal")
}

func TestReadWriteLargeObject(t *testing.T) {
	startTestServer(t)
	numberOfEntries := 1
	objectBytes, err := writeLarge("test", numberOfEntries, 50_000)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := read("test", 0, uint32(numberOfEntries))
	if err != nil {
		t.Fatal(err)
	}
	if !assert.Equal(t, numberOfEntries, len(entries), "should be equal") {
		t.FailNow()
	}
	actualObjectSize := len(entries[0].Content)
	assert.Equal(t, actualObjectSize, objectBytes, "should be equal")
}

func TestReadWriteVerification(t *testing.T) {
	startTestServer(t)
	numberOfEntries := 10000
	err := write("test", numberOfEntries, 100)
	assert.Nil(t, err)
	entries, err := read("test", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, numberOfEntries, len(entries), "should be equal")
}

// Reads from every offset of a topic spanning several blocks, and checks each read returns
// exactly the entries from that offset to the end of the log, in order.
func TestReadWriteWithOffsetVerification(t *testing.T) {
	startTestServer(t)
	batches, batchSize := 10, 100
	for i := 0; i < batches; i++ {
		if err := write("test", batchSize, 100); err != nil {
			t.Fatal(err)
		}
	}
	writeEntries := batches * batchSize
	for offset := 0; offset < writeEntries; offset++ {
		entries, err := read("test", uint64(offset), 10)
		if err != nil {
			t.Fatalf("read from offset %d: %v", offset, err)
		}
		if len(entries) != writeEntries-offset {
			t.Fatalf("read from offset %d returned %d entries, want %d", offset, len(entries), writeEntries-offset)
		}
		for i, entry := range entries {
			if entry.Offset != uint64(offset+i) {
				t.Fatalf("read from offset %d: entry %d has offset %d", offset, i, entry.Offset)
			}
		}
	}
}

func list() (*grpcApi.TopicList, error) {
	client, err := newIbsenClient(ibsenTestTarget)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Client.List(ctx, &grpcApi.EmptyArgs{})
}

func writeLarge(topic string, numberOfEntries int, entryKb int) (int, error) {
	client, err := newIbsenClient(ibsenTestTarget)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entries, size := createLargeInputEntries(topic, numberOfEntries, entryKb)
	_, err = client.Client.Write(ctx, &entries)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func write(topic string, numberOfEntries int, entryByteSize int) error {
	client, err := newIbsenClient(ibsenTestTarget)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entries := createInputEntries(topic, numberOfEntries, entryByteSize)
	_, err = client.Client.Write(ctx, &entries)
	return err
}

func read(topic string, offset uint64, batchSize uint32) ([]*grpcApi.Entry, error) {
	client, err := newIbsenClient(ibsenTestTarget)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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

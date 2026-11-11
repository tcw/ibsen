package grpcapi

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newRetainedServer writes one entry per block and removes all but the last two blocks.
func newRetainedServer(t *testing.T) (server, int) {
	t.Helper()
	logManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		Store: filestore.NewOS(t.TempDir()), MaxBlockSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logManager.Close() })
	s := server{manager: &logManager, TTL: time.Minute, CheckForNewEvery: time.Millisecond}
	for i := 0; i < 5; i++ {
		writeTestEntries(t, s, i, 1)
	}
	keepTwo := func(domain.TopicName) topic.RetentionPolicy { return topic.RetentionPolicy{MaxBytes: 1} }
	if err := logManager.Retain(keepTwo, time.Now()); err != nil {
		t.Fatal(err)
	}
	return s, 4
}

func TestRead_aDeletedOffsetIsOutOfRange(t *testing.T) {
	s, earliest := newRetainedServer(t)
	c := &collector{}
	stream := &fakeReadStream{ctx: context.Background(), send: c.send}
	err := waitForRead(t, startRead(s, &ReadParams{Topic: testTopic, Offset: 0, BatchSize: 10, StopOnCompletion: true}, stream))
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("got %v, want OUT_OF_RANGE", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "earliest available is 4") {
		t.Errorf("the error does not name the earliest offset: %v", err)
	}
	_ = earliest
}

func TestRead_fromEarliestStartsAtWhatIsLeft(t *testing.T) {
	s, earliest := newRetainedServer(t)
	c := &collector{}
	stream := &fakeReadStream{ctx: context.Background(), send: c.send}
	params := &ReadParams{Topic: testTopic, Offset: 0, BatchSize: 10, StopOnCompletion: true, FromEarliest: true}
	if err := waitForRead(t, startRead(s, params, stream)); err != nil {
		t.Fatal(err)
	}
	if err := c.checkContiguous(earliest, 5); err != nil {
		t.Fatal(err)
	}
}

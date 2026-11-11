package grpcapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrite_aRetryIsAnsweredAsADuplicate(t *testing.T) {
	s := newTestServer(t, 3)
	producer := make([]byte, 16)
	producer[0] = 7
	write := &InputEntries{Topic: testTopic, Entries: [][]byte{[]byte("a"), []byte("b")}, ProducerId: producer, Sequence: 1}
	first, err := s.Write(context.Background(), write)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.FirstOffset != 3 || first.Wrote != 2 {
		t.Fatalf("first write %+v", first)
	}
	again, err := s.Write(context.Background(), write)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.FirstOffset != 3 || again.Wrote != 2 {
		t.Fatalf("the retry got %+v, want a duplicate at 3", again)
	}
}

func TestWrite_aBadSequenceIsFailedPrecondition(t *testing.T) {
	s := newTestServer(t, 0)
	producer := make([]byte, 16)
	producer[0] = 7
	if _, err := s.Write(context.Background(), &InputEntries{Topic: testTopic, Entries: [][]byte{[]byte("a")}, ProducerId: producer, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Write(context.Background(), &InputEntries{Topic: testTopic, Entries: [][]byte{[]byte("c")}, ProducerId: producer, Sequence: 3})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a gap got %v, want FAILED_PRECONDITION", err)
	}
}

func TestWrite_aProducerIdThatIsNot16BytesIsInvalid(t *testing.T) {
	s := newTestServer(t, 0)
	_, err := s.Write(context.Background(), &InputEntries{Topic: testTopic, Entries: [][]byte{[]byte("a")}, ProducerId: []byte("short"), Sequence: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want INVALID_ARGUMENT", err)
	}
}

package wiring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
	"github.com/tcw/ibsen/wiring/embedded"
)

func TestFlushModeNamesResolve(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		mode     topic.FlushMode
		tick     time.Duration
	}{
		{"", 0, topic.FlushLenient, DefaultFlushInterval},
		{FlushLenient, 0, topic.FlushLenient, DefaultFlushInterval},
		{FlushLenient, 250 * time.Millisecond, topic.FlushLenient, 250 * time.Millisecond},
		{FlushStrict, 0, topic.FlushStrict, 0},
		{FlushStrict, 5 * time.Millisecond, topic.FlushStrict, 5 * time.Millisecond},
	} {
		mode, tick, err := resolveFlushMode(tc.name, tc.interval)
		if err != nil || mode != tc.mode || tick != tc.tick {
			t.Errorf("%q, %s resolved to %s, %s, %v; want %s, %s", tc.name, tc.interval, mode, tick, err, tc.mode, tc.tick)
		}
	}
	if _, _, err := resolveFlushMode("eventually", 0); !errors.Is(err, ErrUnknownFlushMode) {
		t.Errorf("an unknown mode got %v, want ErrUnknownFlushMode", err)
	}
	if _, _, err := resolveFlushMode(FlushLenient, -time.Second); err == nil {
		t.Error("a negative interval was accepted")
	}
}

func startOnDisk(t *testing.T, ibs *IbsenServer) (grpcapi.IbsenClient, func()) {
	t.Helper()
	ibs.Lock = &slowReleaseLock{released: make(chan struct{}), onRelease: func() {}}
	ibs.TTL = time.Minute
	if ibs.MaxBlockSize == 0 {
		ibs.MaxBlockSize = 1 << 20
	}
	ibs.Compression = "none"
	return startInMemory(t, ibs)
}

func flushTickerRunning(ibs *IbsenServer) bool {
	ibs.mu.Lock()
	defer ibs.mu.Unlock()
	return ibs.stopFlushing != nil
}

func TestTheServerTicksInLenientModeOnly(t *testing.T) {
	lenient := &IbsenServer{RootPath: t.TempDir(), FlushMode: FlushLenient}
	_, shutdown := startOnDisk(t, lenient)
	if !flushTickerRunning(lenient) {
		t.Error("a lenient server started no flush ticker")
	}
	shutdown()

	strict := &IbsenServer{RootPath: t.TempDir(), FlushMode: FlushStrict}
	_, shutdown = startOnDisk(t, strict)
	if flushTickerRunning(strict) {
		t.Error("a strict server started a flush ticker")
	}
	shutdown()
}

func TestAnUnknownFlushModeIsRefusedAtStart(t *testing.T) {
	ibs := &IbsenServer{RootPath: t.TempDir(), FlushMode: "sometimes", Compression: "none",
		Lock: &slowReleaseLock{released: make(chan struct{}), onRelease: func() {}}}
	err := ibs.Start(nil)
	if !errors.Is(err, ErrUnknownFlushMode) {
		t.Fatalf("got %v, want ErrUnknownFlushMode", err)
	}
}

// A tick an hour away has not happened when the server stops: the shutdown is what flushes,
// and the restarted log holds the write.
func TestACleanShutdownFlushesWhatTheTickHadNot(t *testing.T) {
	root := t.TempDir()
	ibs := &IbsenServer{RootPath: root, FlushMode: FlushLenient, FlushInterval: time.Hour}
	client, shutdown := startOnDisk(t, ibs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Write(ctx, &grpcapi.InputEntries{Topic: "t", Entries: [][]byte{[]byte("one")}}); err != nil {
		t.Fatal(err)
	}
	shutdown()

	reopened, err := embedded.Open(embedded.Params{Store: filestore.NewOS(root)})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := countEntries(t, reopened, "t"); got != 1 {
		t.Fatalf("%d entries after a clean shutdown, want 1", got)
	}
}

func TestOpenLocalLenientCloseMakesEverythingDurable(t *testing.T) {
	root := t.TempDir()
	local, err := OpenLocal(LocalParams{RootPath: root, FlushMode: FlushLenient, FlushInterval: time.Hour, LockMode: LockNone})
	if err != nil {
		t.Fatal(err)
	}
	entries := [][]byte{[]byte("a"), []byte("b")}
	if _, err := local.Write(driver.WriteParams{Topic: "t", Entries: &entries}); err != nil {
		t.Fatal(err)
	}
	if got := countEntries(t, local, "t"); got != 0 {
		t.Errorf("%d entries readable before the flush", got)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenLocal(LocalParams{RootPath: root, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := countEntries(t, reader, "t"); got != 2 {
		t.Fatalf("%d entries after close, want 2", got)
	}
}

func countEntries(t *testing.T, log driver.LogManager, name string) int {
	t.Helper()
	return len(readEntries(t, log, name))
}

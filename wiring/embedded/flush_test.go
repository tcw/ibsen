package embedded

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
)

// An embedded build is woken by nothing it did not ask for.
func TestOpenStartsNoGoroutineByDefault(t *testing.T) {
	for _, params := range []Params{
		{},
		// lenient with no interval leaves Flush to the program
		{FlushMode: topic.FlushLenient},
	} {
		params.Store = filestore.NewOS(t.TempDir())
		before := runtime.NumGoroutine()
		log, err := Open(params)
		if err != nil {
			t.Fatal(err)
		}
		after := runtime.NumGoroutine()
		log.Close()
		if after > before {
			t.Errorf("Open(%+v) started %d goroutines, want none", params.FlushMode, after-before)
		}
	}
}

func TestLenientWithAnIntervalFlushesOnItsOwn(t *testing.T) {
	log := openTestLog(t, Params{
		Store: filestore.NewOS(t.TempDir()), FlushMode: topic.FlushLenient, FlushInterval: time.Millisecond,
	})
	write(t, log, "t", 0, 3)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := tryReadAll(log, "t"); got == 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the ticker never made the entries readable")
}

func TestLenientWithoutAnIntervalWaitsForFlush(t *testing.T) {
	log := openTestLog(t, Params{Store: filestore.NewOS(t.TempDir()), FlushMode: topic.FlushLenient})
	write(t, log, "t", 0, 3)
	time.Sleep(20 * time.Millisecond)
	if got := tryReadAll(log, "t"); got != 0 {
		t.Fatalf("%d entries readable before anything flushed them", got)
	}
	if err := log.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := tryReadAll(log, "t"); got != 3 {
		t.Fatalf("%d entries readable after Flush, want 3", got)
	}
}

func TestCloseFlushesAndReportsAFailure(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(Params{Store: filestore.NewOS(dir), FlushMode: topic.FlushLenient})
	if err != nil {
		t.Fatal(err)
	}
	write(t, log, "t", 0, 2)
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestLog(t, Params{Store: filestore.NewOS(dir)})
	if got := len(readAll(t, reopened, "t", 0)); got != 2 {
		t.Fatalf("%d entries after a clean close, want 2", got)
	}
}

func TestStartFlushTickerStopsAndReportsErrors(t *testing.T) {
	var calls, reported atomic.Int32
	failing := flusherFunc(func() error { calls.Add(1); return errors.New("EIO") })
	stop := StartFlushTicker(failing, time.Millisecond, func(error) { reported.Add(1) })
	deadline := time.Now().Add(10 * time.Second)
	for reported.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()
	stopped := calls.Load()
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != stopped {
		t.Error("the ticker kept flushing after stop returned")
	}
	if reported.Load() < 3 {
		t.Error("failed flushes were not reported")
	}
	stop() // a second stop is harmless
}

type flusherFunc func() error

func (f flusherFunc) Flush() error { return f() }

// tryReadAll counts what a read of the topic returns, treating "nothing yet" as zero.
func tryReadAll(log *Log, name string) int {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	got := 0
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			got += len(*batch)
			wg.Done()
		}
		close(done)
	}()
	err := log.Read(driver.ReadParams{TopicName: domain.TopicName(name), LogChan: logChan, Wg: &wg, BatchSize: 10})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		return 0
	}
	return got
}

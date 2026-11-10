package zstd

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/topic"
)

// Dictionaries, end to end: a topic trains one from its own entries off the write path, writes
// with it, reads it back, and keeps it while a frame names it.

func event(shape string, i int) []byte {
	if shape == "orders" {
		return []byte(fmt.Sprintf(`{"event":"order-placed","order":%d,"customer":"cust-%04d","amount":%d,"currency":"NOK","channel":"web"}`, i, i%97, i%500))
	}
	return []byte(fmt.Sprintf(`<metric name="cpu.load" host="node-%02d" dc="osl-1" value="%d.%02d" unit="percent" at="%d"/>`, i%13, i%100, i%97, 1700000000+i))
}

func writeEvents(t *testing.T, tp *topic.Topic, shape string, from, count, perWrite int) {
	t.Helper()
	for i := from; i < from+count; i += perWrite {
		var batch [][]byte
		for j := i; j < i+perWrite && j < from+count; j++ {
			batch = append(batch, event(shape, j))
		}
		if err := tp.Write(&batch); err != nil {
			t.Fatal(err)
		}
	}
}

func openDictTopic(t *testing.T, store driven.BlockStore, codec *Codec, dictionaries bool, blockSize int) *topic.Topic {
	t.Helper()
	tp := topic.NewLogTopic(topic.Params{Store: store, TopicName: "t", MaxBlockSize: blockSize, Codec: codec,
		Codecs: driven.NewCodecs(codec), MaxFrameEntries: 10, Dictionaries: dictionaries, DictTrainBytes: 32 << 10})
	if err := tp.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tp.Close() })
	return tp
}

func dictsInStore(t *testing.T, store driven.BlockStore) []uint64 {
	t.Helper()
	blocks, err := store.List("t", driven.Dict)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for _, block := range blocks {
		ids = append(ids, block.Block)
	}
	return ids
}

func dictsNamed(t *testing.T, store driven.BlockStore) []uint64 {
	t.Helper()
	blocks, _ := store.List("t", driven.Log)
	var ids []uint64
	for _, block := range blocks {
		named, err := logfmt.ScanDictionaries(store, driven.LogRef("t", domain.LogBlock(block.Block)))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, named...)
	}
	return ids
}

// writeUntilNamed writes until a frame names a dictionary: training runs off the write path,
// so the write that crosses the threshold does not wait for it.
func writeUntilNamed(t *testing.T, tp *topic.Topic, store driven.BlockStore, shape string, from int) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		writeEvents(t, tp, shape, from, 100, 10)
		from += 100
		if len(dictsNamed(t, store)) > 0 {
			return from
		}
	}
	t.Fatal("no frame ever named a dictionary")
	return from
}

func dictCodec(t *testing.T) *Codec {
	t.Helper()
	codec, err := New(Default)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(codec.Close)
	return codec
}

func checkAll(t *testing.T, tp *topic.Topic, want func(i int) []byte, count int) {
	t.Helper()
	got := readFrom(t, tp, 0)
	if len(got) != count {
		t.Fatalf("read %d entries, want %d", len(got), count)
	}
	for i, entry := range got {
		if string(entry.Entry) != string(want(i)) {
			t.Fatalf("offset %d holds %q, want %q", i, entry.Entry, want(i))
		}
	}
}

func TestDictionary_isTrainedAtTheThresholdAndRoundTrips(t *testing.T) {
	store := filestore.NewOS(t.TempDir())
	codec := dictCodec(t)
	tp := openDictTopic(t, store, codec, true, 1<<20)

	// below the threshold nothing is trained
	writeEvents(t, tp, "orders", 0, 100, 10)
	if ids := dictsInStore(t, store); len(ids) != 0 {
		t.Fatalf("a dictionary was trained after %d entries: %v", 100, ids)
	}
	written := writeUntilNamed(t, tp, store, "orders", 100)
	checkAll(t, tp, func(i int) []byte { return event("orders", i) }, written)

	// and after a restart, with the dictionary read back from the store
	tp.Close()
	reopened := openDictTopic(t, store, dictCodec(t), true, 1<<20)
	checkAll(t, reopened, func(i int) []byte { return event("orders", i) }, written)
	writeEvents(t, reopened, "orders", written, 50, 10)
	checkAll(t, reopened, func(i int) []byte { return event("orders", i) }, written+50)
}

func TestDictionary_offWritesPlainFrames(t *testing.T) {
	store := filestore.NewOS(t.TempDir())
	tp := openDictTopic(t, store, dictCodec(t), false, 1<<20)
	writeEvents(t, tp, "orders", 0, 3000, 10)
	time.Sleep(50 * time.Millisecond)
	if ids := dictsInStore(t, store); len(ids) != 0 {
		t.Fatalf("dictionaries off trained %v", ids)
	}
}

// uselessDicts is zstd whose dictionaries never help: training gives back a dictionary, and
// encoding with it is no better than without.
type uselessDicts struct{ *Codec }

func (u uselessDicts) EncodeDict(dst, src []byte, _ driven.Dictionary) ([]byte, error) {
	return u.Codec.Encode(dst, src)
}

func TestDictionary_thatDoesNotPayIsDiscarded(t *testing.T) {
	store := filestore.NewOS(t.TempDir())
	codec := uselessDicts{dictCodec(t)}
	tp := topic.NewLogTopic(topic.Params{Store: store, TopicName: "t", MaxBlockSize: 1 << 20, Codec: codec,
		Codecs: driven.NewCodecs(codec), MaxFrameEntries: 10, Dictionaries: true, DictTrainBytes: 32 << 10})
	if err := tp.LoadOrCreate(); err != nil {
		t.Fatal(err)
	}
	writeEvents(t, tp, "orders", 0, 3000, 10)
	tp.Close() // waits for training
	if ids := dictsInStore(t, store); len(ids) != 0 {
		t.Errorf("a dictionary that did not pay was kept: %v", ids)
	}
	if ids := dictsNamed(t, store); len(ids) != 0 {
		t.Errorf("frames name a dictionary that did not pay: %v", ids)
	}
}

// When the entries change shape the dictionary stops paying, and a new one is trained; one
// block then holds frames of two dictionaries, and reads back whole.
func TestDictionary_isRetrainedWhenTheDataChangesShape(t *testing.T) {
	store := filestore.NewOS(t.TempDir())
	tp := openDictTopic(t, store, dictCodec(t), true, 256<<10)
	written := writeUntilNamed(t, tp, store, "orders", 0)
	shapeAt := written

	deadline := time.Now().Add(30 * time.Second)
	for len(dictsInStore(t, store)) < 2 && time.Now().Before(deadline) {
		writeEvents(t, tp, "metrics", written, 200, 10)
		written += 200
	}
	if ids := dictsInStore(t, store); len(ids) < 2 {
		t.Fatalf("no second dictionary after the data changed shape: %v", ids)
	}
	writeEvents(t, tp, "metrics", written, 200, 10)
	written += 200
	want := func(i int) []byte {
		if i < shapeAt {
			return event("orders", i)
		}
		return event("metrics", i)
	}
	checkAll(t, tp, want, written)
	named := map[uint64]bool{}
	for _, id := range dictsNamed(t, store) {
		named[id] = true
	}
	if len(named) < 2 {
		t.Errorf("frames name %d dictionaries, want both", len(named))
	}
}

func TestDictionary_missingOrCorruptIsReportedAndNothingIsTruncated(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			store := filestore.NewOS(dir)
			tp := openDictTopic(t, store, dictCodec(t), true, 1<<20)
			written := writeUntilNamed(t, tp, store, "orders", 0)
			tp.Close()
			id := dictsNamed(t, store)[0]
			before, _ := store.List("t", driven.Log)

			ref := driven.DictRef("t", id)
			if damage == "missing" {
				if err := store.Remove(ref); err != nil {
					t.Fatal(err)
				}
			} else {
				blocks, _ := store.List("t", driven.Dict)
				if err := store.Truncate(ref, blocks[0].Size-1); err != nil {
					t.Fatal(err)
				}
			}

			reopened := openDictTopic(t, store, dictCodec(t), true, 1<<20)
			after, _ := store.List("t", driven.Log)
			if before[len(before)-1].Size != after[len(after)-1].Size {
				t.Fatalf("loading truncated the log from %d to %d bytes", before[len(before)-1].Size, after[len(after)-1].Size)
			}
			err := readErr(reopened, domain.Offset(written-1))
			want := driven.ErrDictionaryNotFound
			if damage == "corrupt" {
				want = driven.ErrCorruptDictionary
			}
			if !errors.Is(err, want) {
				t.Fatalf("a read of a frame naming a %s dictionary got %v, want %v", damage, err, want)
			}
			if wantID := fmt.Sprintf("%016x", id); !strings.Contains(err.Error(), wantID) {
				t.Errorf("the error does not name dictionary %s: %v", wantID, err)
			}
			// the frames from before the dictionary still read, up to the first that names it
			if got, err := readSome(reopened, 0); got == 0 || !errors.Is(err, want) {
				t.Errorf("a read from the start got %d entries and %v; want the plain frames, then the error", got, err)
			}
		})
	}
}

// A dictionary is written and made durable before any frame names it. A crash in between
// leaves a dictionary no frame names: loading is fine, and writing carries on.
func TestDictionary_aCrashBetweenTheDictionaryAndItsFirstFrame(t *testing.T) {
	dir := t.TempDir()
	store := filestore.NewOS(dir)
	codec := dictCodec(t)
	var samples [][]byte
	for i := 0; i < 500; i++ {
		samples = append(samples, event("orders", i))
	}
	content, err := codec.Train(samples, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	// what the crash leaves: a topic with plain frames, and a dictionary nobody used
	tp := openDictTopic(t, store, codec, true, 1<<20)
	writeEvents(t, tp, "orders", 0, 20, 10)
	tp.Close()
	if _, err := store.Append(driven.DictRef("t", 77), append([]byte{0, 0, 0, 0}, content...)); err != nil {
		t.Fatal(err)
	}

	reopened := openDictTopic(t, store, dictCodec(t), true, 1<<20)
	checkAll(t, reopened, func(i int) []byte { return event("orders", i) }, 20)
	written := writeUntilNamed(t, reopened, store, "orders", 20)
	checkAll(t, reopened, func(i int) []byte { return event("orders", i) }, written)
}

// Retention removes a dictionary only once no block left names it.
func TestDictionary_retentionKeepsWhatIsStillNamed(t *testing.T) {
	store := filestore.NewOS(t.TempDir())
	tp := openDictTopic(t, store, dictCodec(t), true, 64<<10)
	written := writeUntilNamed(t, tp, store, "orders", 0)
	writeEvents(t, tp, "orders", written, 2000, 10)
	first := dictsInStore(t, store)

	// keep only the head: the blocks that named the dictionary go, but it is still the one
	// being written with, and the head names it
	if _, err := tp.Retain(topic.RetentionPolicy{MaxBytes: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := dictsInStore(t, store); fmt.Sprint(got) != fmt.Sprint(first) {
		t.Fatalf("dictionaries %v after retention, want %v kept", got, first)
	}
	// an unused one, as a crash would leave, goes
	if _, err := store.Append(driven.DictRef("t", 12345), []byte("not named by anything")); err != nil {
		t.Fatal(err)
	}
	writeEvents(t, tp, "orders", written+2000, 2000, 10)
	if _, err := tp.Retain(topic.RetentionPolicy{MaxBytes: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, id := range dictsInStore(t, store) {
		if id == 12345 {
			t.Error("retention kept a dictionary no block names")
		}
	}
}

func readSome(tp *topic.Topic, from domain.Offset) (int, error) {
	logChan := make(chan *[]domain.LogEntry)
	done := make(chan struct{})
	var wg sync.WaitGroup
	got := 0
	go func() {
		for batch := range logChan {
			got += len(*batch)
			wg.Done()
		}
		close(done)
	}()
	err := tp.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg, From: from, BatchSize: 1})
	wg.Wait()
	close(logChan)
	<-done
	return got, err
}

func readErr(tp *topic.Topic, from domain.Offset) error {
	logChan := make(chan *[]domain.LogEntry)
	done := make(chan struct{})
	var wg syncWaitGroup
	go func() {
		for range logChan {
			wg.Done()
		}
		close(done)
	}()
	err := tp.Read(domain.ReadLogParams{LogChan: logChan, Wg: &wg.WaitGroup, From: from, BatchSize: 100})
	wg.Wait()
	close(logChan)
	<-done
	return err
}

type syncWaitGroup struct{ sync.WaitGroup }

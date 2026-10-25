package cli

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/wiring"
)

// TestReadLogFile_readsABlockWrittenWithTheDefaultCodec is the hole the default compression
// opened and this closes. `read-log` opens a block file outside any store, so nothing hands
// it a codec registry; with the default codec being zstd, a block written by an ordinary
// append would have come back as an unknown codec rather than as its entries.
func TestReadLogFile_readsABlockWrittenWithTheDefaultCodec(t *testing.T) {
	root := t.TempDir()
	local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	// enough entries, and repetitive enough, that zstd pays and the frame is really stored
	// compressed: a frame a codec could not shrink falls back to the plain bytes and would
	// read back without any registry at all, which would make this test pass for the wrong
	// reason
	entries := make([][]byte, 1000)
	for i := range entries {
		entries[i] = []byte(fmt.Sprintf(
			`{"event":"order-placed","id":%d,"customer":"cust-%04d","currency":"NOK"}`, i, i%50))
	}
	if err := local.Write(domain.TopicName("topic"), &entries); err != nil {
		t.Fatal(err)
	}
	local.Close()

	codecs, closeCodecs, err := wiring.ReadCodecs()
	if err != nil {
		t.Fatal(err)
	}
	defer closeCodecs()

	printed := capturingStdout(t, func() {
		block := filepath.Join(root, "topic", "00000000000000000000.log")
		if err := ReadLogFile(block, 10, codecs); err != nil {
			t.Fatalf("reading a block written with the default codec: %v", err)
		}
	})

	for _, want := range []string{`"id":0,`, `"id":999,`} {
		if !strings.Contains(printed, want) {
			t.Fatalf("read-log printed %q, want it to hold %q", printed, want)
		}
	}
}

// capturingStdout runs body with os.Stdout on a pipe and returns what was written to it,
// since printing the entries is what this command is.
func capturingStdout(t *testing.T, body func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = writer
	done := make(chan string, 1)
	go func() {
		out, _ := io.ReadAll(reader)
		done <- string(out)
	}()
	body()
	os.Stdout = stdout
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return <-done
}

// TestReadLogIndexFile_reportsWhatItCouldNotRead pins the one thing a diagnostic must not do,
// which is print a short index as if it were the whole of it. Parsing stops at a torn pair,
// as a load does, and the bytes behind it are named rather than dropped.
func TestReadLogIndexFile_reportsWhatItCouldNotRead(t *testing.T) {
	root := t.TempDir()
	local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: root})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		entries := [][]byte{[]byte(fmt.Sprintf("entry-%d", i))}
		if err := local.Write(domain.TopicName("topic"), &entries); err != nil {
			t.Fatal(err)
		}
	}
	local.Close()

	indexFile := filepath.Join(root, "topic", "00000000000000000000.idx")
	clean := capturingStdout(t, func() {
		if err := ReadLogIndexFile(indexFile); err != nil {
			t.Fatal(err)
		}
	})
	for _, want := range []string{"0 -> 0\n", "10 -> ", "20 -> "} {
		if !strings.Contains(clean, want) {
			t.Fatalf("read-index printed %q, want it to hold %q", clean, want)
		}
	}
	if strings.Contains(clean, "torn or corrupt") {
		t.Fatalf("read-index reported damage in a clean index: %q", clean)
	}

	file, err := os.OpenFile(indexFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("half a pair")); err != nil {
		t.Fatal(err)
	}
	file.Close()
	torn := capturingStdout(t, func() {
		if err := ReadLogIndexFile(indexFile); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.HasPrefix(torn, clean) {
		t.Fatalf("a torn tail changed the pairs printed before it:\n%q\nwant prefix\n%q", torn, clean)
	}
	if !strings.Contains(torn, "11 bytes after the last good pair are torn or corrupt") {
		t.Fatalf("read-index printed %q, want it to report the 11 bytes it could not read", torn)
	}
}

func TestReadLogIndexFile_returnsAMissingFile(t *testing.T) {
	if err := ReadLogIndexFile(filepath.Join(t.TempDir(), "missing.idx")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got %v, want an error wrapping fs.ErrNotExist", err)
	}
}

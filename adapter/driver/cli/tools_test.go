package cli

import (
	"fmt"
	"io"
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

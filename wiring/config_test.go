package wiring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/core/topic"
)

func writeConfig(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, ConfigFileName)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRetentionSizesAndAgesParse(t *testing.T) {
	sizes := map[string]int64{
		"": 0, "0": 0, "1024": 1024, "10GB": 10_000_000_000, "512MiB": 512 << 20, "1.5 kib": 1536,
		"50gb": 50_000_000_000, "2T": 2_000_000_000_000,
	}
	for in, want := range sizes {
		if got, err := ParseBytes(in); err != nil || got != want {
			t.Errorf("ParseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"10QB", "-5", "GB", "lots"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q) was accepted", bad)
		}
	}
	ages := map[string]time.Duration{"": 0, "0": 0, "168h": 168 * time.Hour, "7d": 7 * 24 * time.Hour, "90m": 90 * time.Minute}
	for in, want := range ages {
		if got, err := ParseAge(in); err != nil || got != want {
			t.Errorf("ParseAge(%q) = %s, %v; want %s", in, got, err, want)
		}
	}
	for _, bad := range []string{"1x", "-1h", "0d", "week"} {
		if _, err := ParseAge(bad); err == nil {
			t.Errorf("ParseAge(%q) was accepted", bad)
		}
	}
}

func TestAnOverrideReplacesTheGlobalLimits(t *testing.T) {
	root := t.TempDir()
	writeConfig(t, root, `{"topics": {"audit": {"retainAge": "720h"}, "metrics": {"retainBytes": "1GB"}}}`)
	retention, err := LoadRetention(root, "", "168h", "10GB")
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]topic.RetentionPolicy{
		"audit":   {MaxAge: 720 * time.Hour},
		"metrics": {MaxBytes: 1_000_000_000},
		"other":   {MaxAge: 168 * time.Hour, MaxBytes: 10_000_000_000},
	} {
		if got := retention.For(domain.TopicName(name)); got != want {
			t.Errorf("%s gets %+v, want %+v", name, got, want)
		}
	}
}

func TestAConfigFileThatCannotBeUsedIsRefused(t *testing.T) {
	for name, content := range map[string]string{
		"unknown top-level key": `{"topic": {}}`,
		"unknown topic key":     `{"topics": {"a": {"retain": "1h"}}}`,
		"bad age":               `{"topics": {"a": {"retainAge": "1x"}}}`,
		"bad size":              `{"topics": {"a": {"retainBytes": "huge"}}}`,
		"bad topic name":        `{"topics": {"../a": {"retainAge": "1h"}}}`,
		"not json":              `retainAge = 1h`,
		"two objects":           `{} {}`,
	} {
		root := t.TempDir()
		writeConfig(t, root, content)
		if _, err := LoadRetention(root, "", "", ""); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: got %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestTheConfigFileIsOptionalUnlessNamed(t *testing.T) {
	root := t.TempDir()
	if _, err := LoadRetention(root, "", "", ""); err != nil {
		t.Errorf("a directory without ibsen.json was refused: %v", err)
	}
	if _, err := LoadRetention(root, filepath.Join(root, "missing.json"), "", ""); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("a named file that is not there got %v, want ErrInvalidConfig", err)
	}
	elsewhere := writeConfig(t, t.TempDir(), `{"topics": {"a": {"retainBytes": "1KB"}}}`)
	retention, err := LoadRetention(root, elsewhere, "", "")
	if err != nil || retention.For("a").MaxBytes != 1000 {
		t.Errorf("--config elsewhere gave %+v, %v", retention.For("a"), err)
	}
}

// A read-only open reads the settings file — a bad one is still refused — and deletes
// nothing, and nothing ever writes the file.
func TestReadOnlyOpenReadsTheConfigButDeletesNothing(t *testing.T) {
	root := t.TempDir()
	writer, err := OpenLocal(LocalParams{RootPath: root, MaxBlockSize: 1, LockMode: LockNone, FlushMode: FlushStrict})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		entries := [][]byte{[]byte("x")}
		if _, err := writer.Write(driver.WriteParams{Topic: "t", Entries: &entries}); err != nil {
			t.Fatal(err)
		}
	}
	writer.Close()
	config := writeConfig(t, root, `{"topics": {"t": {"retainBytes": "1"}}}`)
	before, _ := os.ReadFile(config)

	reader, err := OpenLocal(LocalParams{RootPath: root, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if blocks, _ := filestore.NewOS(root).List("t", driven.Log); len(blocks) != 4 {
		t.Errorf("a read-only open left %d blocks, want 4", len(blocks))
	}
	after, _ := os.ReadFile(config)
	if string(before) != string(after) {
		t.Error("the settings file changed")
	}

	writeConfig(t, root, `{"topics": {"t": {"retainBytes": "huge"}}}`)
	if _, err := OpenLocal(LocalParams{RootPath: root, ReadOnly: true}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("a read-only open accepted a bad settings file: %v", err)
	}
}

// append applies retention when it finishes, from the same file the server reads.
func TestAWritableOpenAppliesRetentionWhenItCloses(t *testing.T) {
	root := t.TempDir()
	writeConfig(t, root, `{"topics": {"t": {"retainBytes": "1"}}}`)
	local, err := OpenLocal(LocalParams{RootPath: root, MaxBlockSize: 1, LockMode: LockNone})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		entries := [][]byte{[]byte("x")}
		if _, err := local.Write(driver.WriteParams{Topic: "t", Entries: &entries}); err != nil {
			t.Fatal(err)
		}
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	if blocks, _ := filestore.NewOS(root).List("t", driven.Log); len(blocks) != 1 {
		t.Errorf("%d blocks after append closed, want only the head", len(blocks))
	}
}

func TestTheServerAppliesRetentionOnATicker(t *testing.T) {
	root := t.TempDir()
	ibs := &IbsenServer{RootPath: root, FlushMode: FlushStrict, MaxBlockSize: 1,
		RetainBytes: "1", RetentionInterval: 5 * time.Millisecond}
	client, shutdown := startOnDisk(t, ibs)
	defer shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 4; i++ {
		if _, err := client.Write(ctx, &grpcapi.InputEntries{Topic: "t", Entries: [][]byte{[]byte("x")}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if blocks, _ := filestore.NewOS(root).List("t", driven.Log); len(blocks) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the retention ticker never removed the old blocks")
}

package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionIsStampedOrDev(t *testing.T) {
	saved := Version
	t.Cleanup(func() { Version = saved })

	Version = "v9.9.9"
	if got := version(); got != "v9.9.9" {
		t.Fatalf("a stamped version must win, got %q", got)
	}
	Version = ""
	// a test binary is built from a working tree, so there is no module version to read
	if got := version(); got != "dev" {
		t.Fatalf("an unstamped build from a working tree is dev, got %q", got)
	}
}

func TestVersionFlagPrintsTheVersion(t *testing.T) {
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{"--version"})
	t.Cleanup(func() { rootCmd.SetOut(nil); rootCmd.SetArgs(nil) })
	if err := rootCmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), rootCmd.Version) {
		t.Fatalf("--version printed %q, want it to name %q", out.String(), rootCmd.Version)
	}
}

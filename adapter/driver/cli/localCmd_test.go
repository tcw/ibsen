package cli

import (
	"testing"

	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/core/domain"
)

func TestParseFraming(t *testing.T) {
	for _, test := range []struct {
		name    string
		want    stdio.Framing
		refused bool
	}{
		{name: "", want: stdio.Lines},
		{name: "lines", want: stdio.Lines},
		{name: "length", want: stdio.Length},
		{name: "Lines", refused: true},
		{name: "netstring", refused: true},
	} {
		got, err := parseFraming(test.name)
		if test.refused {
			if err == nil {
				t.Errorf("framing %q was accepted as %v, want an error naming what is allowed", test.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("framing %q gave %v", test.name, err)
			continue
		}
		if got != test.want {
			t.Errorf("framing %q gave %v, want %v", test.name, got, test.want)
		}
	}
}

// TestParseOffsetArg: an offset that is not a number is an error rather than a zero, for the
// reason parseReadArgs has a test of its own — strconv.ParseUint returns 0 on failure, so
// carrying on turns a mistyped offset into a read of the whole topic.
func TestParseOffsetArg(t *testing.T) {
	if got, err := parseOffsetArg([]string{"topic"}); err != nil || got != 0 {
		t.Errorf("no offset gave (%d, %v), want (0, nil)", got, err)
	}
	if got, err := parseOffsetArg([]string{"topic", "42"}); err != nil || got != 42 {
		t.Errorf("offset 42 gave (%d, %v), want (42, nil)", got, err)
	}
	if _, err := parseOffsetArg([]string{"topic", "tail"}); err == nil {
		t.Error("offset \"tail\" was accepted, want an error rather than a read from zero")
	}
	if _, err := parseOffsetArg([]string{"topic", "-1"}); err == nil {
		t.Error("offset -1 was accepted, want an error")
	}
}

// TestLocalRootIsRequired: these commands work on a data directory and there is no in-memory
// fallback for them, so a missing one is refused rather than defaulted.
func TestLocalRootIsRequired(t *testing.T) {
	previous := rootDirectory
	t.Cleanup(func() { rootDirectory = previous })

	rootDirectory = ""
	if _, err := localRoot(); err == nil {
		t.Error("an empty data directory was accepted")
	}
	rootDirectory = t.TempDir()
	if got, err := localRoot(); err != nil || got != rootDirectory {
		t.Errorf("localRoot gave (%q, %v), want the directory given", got, err)
	}
}

func TestHoldsTopic(t *testing.T) {
	names := []domain.TopicName{"alpha", "beta"}
	if !holdsTopic(names, "beta") {
		t.Error("beta was not found among the topics")
	}
	if holdsTopic(names, "gamma") {
		t.Error("gamma was found among topics that do not hold it")
	}
}

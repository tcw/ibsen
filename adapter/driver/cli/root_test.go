package cli

import (
	"strings"
	"testing"
)

// The bug this pins: "client read <topic> <offset> <batchSize>" parsed the batch size and
// left the offset at zero, so asking for the tail of a topic silently read all of it. Two
// arguments worked, which is why it went unnoticed.
func TestParseReadArgs(t *testing.T) {
	for _, test := range []struct {
		name          string
		args          []string
		wantOffset    uint64
		wantBatchSize uint32
	}{
		{name: "topic only", args: []string{"orders"}, wantOffset: 0, wantBatchSize: defaultReadBatchSize},
		{name: "offset", args: []string{"orders", "295"}, wantOffset: 295, wantBatchSize: defaultReadBatchSize},
		{name: "offset and batch size", args: []string{"orders", "295", "50"}, wantOffset: 295, wantBatchSize: 50},
		{name: "offset zero is still parsed", args: []string{"orders", "0", "50"}, wantOffset: 0, wantBatchSize: 50},
	} {
		t.Run(test.name, func(t *testing.T) {
			offset, batchSize, err := parseReadArgs(test.args)

			if err != nil {
				t.Fatal(err)
			}
			if offset != test.wantOffset {
				t.Errorf("offset is %d, want %d", offset, test.wantOffset)
			}
			if batchSize != test.wantBatchSize {
				t.Errorf("batch size is %d, want %d", batchSize, test.wantBatchSize)
			}
		})
	}
}

// An argument that is not a number is refused. ParseUint returns 0 on failure, so accepting
// it would turn a mistyped offset into a read of the whole topic, and a mistyped batch size
// into one the core rejects further down with a worse message.
func TestParseReadArgsRefusesWhatItCannotParse(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		says string
	}{
		{name: "offset is not a number", args: []string{"orders", "tail"}, says: `offset "tail"`},
		{name: "offset is negative", args: []string{"orders", "-1"}, says: `offset "-1"`},
		{name: "batch size is not a number", args: []string{"orders", "0", "all"}, says: `batch size "all"`},
		{name: "batch size is zero", args: []string{"orders", "0", "0"}, says: "greater than zero"},
		{name: "too many arguments", args: []string{"orders", "0", "10", "extra"}, says: "got 4 arguments"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := parseReadArgs(test.args)

			if err == nil {
				t.Fatalf("%v was accepted", test.args)
			}
			if !strings.Contains(err.Error(), test.says) {
				t.Errorf("error %q does not say %q, so it names the wrong argument", err, test.says)
			}
		})
	}
}

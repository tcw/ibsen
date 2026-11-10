package stdio_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/tcw/ibsen/adapter/driver/stdio"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
)

// failsAfter passes writes through and then fails the nth, after it has been stored: the
// pipeline dies not knowing whether its last batch landed.
type failsAfter struct {
	driver.LogManager
	n, writes int
}

func (f *failsAfter) Write(params driver.WriteParams) (domain.WriteResult, error) {
	f.writes++
	result, err := f.LogManager.Write(params)
	if f.writes == f.n {
		return domain.WriteResult{}, errors.New("the connection went away")
	}
	return result, err
}

// Rerunning a failed append of the same input with the same producer finishes the job and
// stores nothing twice, wherever the first run died.
func TestRerunningAnAppendWithAProducerIsSafe(t *testing.T) {
	input := strings.Repeat("line\n", 9) + "last\n"
	params := stdio.AppendParams{BatchSize: 3, Producer: domain.ProducerID{42}}
	for failAt := 1; failAt <= 4; failAt++ {
		log := openLog(t)
		if _, err := stdio.Append(&failsAfter{LogManager: log, n: failAt}, "t", strings.NewReader(input), params); err == nil {
			t.Fatalf("fail at %d: the first run did not fail", failAt)
		}
		appendString(t, log, "t", input, params)
		if out := catString(t, log, "t", stdio.CatParams{}); out != input {
			t.Fatalf("fail at %d: after the rerun the topic holds %q, want the input once", failAt, out)
		}
		if again := appendString(t, log, "t", input, params); again != 0 {
			t.Errorf("fail at %d: a third run of a finished input wrote %d entries", failAt, again)
		}
	}
}

// Without a producer the same rerun stores the batches the first run got through twice.
func TestRerunningWithoutAProducerDuplicates(t *testing.T) {
	log := openLog(t)
	input := "a\nb\nc\nd\n"
	params := stdio.AppendParams{BatchSize: 2}
	_, _ = stdio.Append(&failsAfter{LogManager: log, n: 2}, "t", strings.NewReader(input), params)
	appendString(t, log, "t", input, params)
	if out := catString(t, log, "t", stdio.CatParams{}); out == input {
		t.Fatal("expected the documented duplicate without a producer")
	}
}

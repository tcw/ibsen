package errore

import (
	"errors"
	"strings"
	"testing"
)

func TestWrapWithContextError(t *testing.T) {
	err := WrapWithContext(errors.New("err1"))
	if err == nil {
		t.FailNow()
	}
	containsFunc := strings.Contains(err.Error(), "TestWrapWithContextError")
	containsErr := strings.Contains(err.Error(), "err1")
	if !(containsFunc && containsErr) {
		t.Fail()
	}
}

func TestRootCause(t *testing.T) {
	err := WrapWithContext(errors.New("err1"))
	err = WrapWithContext(err)
	err = WrapWithContext(err)
	root := RootCause(err)
	if root != nil && root.Error() != "err1" {
		t.Fail()
	}
}

func TestErrorTrace(t *testing.T) {
	err := WrapWithContext(errors.New("err1"))
	err = WrapWithContext(err)
	err = WrapWithContext(err)

	trace := Trace(err)
	if trace[0] != "err1" {
		t.Fail()
	}
}

func TestErrorTraceSprint(t *testing.T) {
	err := WrapWithContext(errors.New("err1"))
	err = WrapWithContext(err)
	err = WrapWithContext(err)

	trace := SprintTrace(err)
	if !strings.Contains(trace, "err1") {
		t.Fail()
	}
}

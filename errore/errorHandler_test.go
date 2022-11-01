package errore

import (
	"errors"
	"strings"
	"testing"
)

func TestWrapWithContextError(t *testing.T) {
	err := Wrap(errors.New("err1"))
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
	err := Wrap(errors.New("err1"))
	err = Wrap(err)
	err = Wrap(err)
	root := RootCause(err)
	if root != nil && root.Error() != "err1" {
		t.Fail()
	}
}

func TestErrorTrace(t *testing.T) {
	err := Wrap(errors.New("err1"))
	err = Wrap(err)
	err = Wrap(err)

	trace := StackTrace(err)
	if trace[0] != "err1" {
		t.Fail()
	}
}

func TestErrorTraceSprint(t *testing.T) {
	err := Wrap(errors.New("err1"))
	err = Wrap(err)
	err = Wrap(err)

	trace := SprintStackTraceNd(err)
	if !strings.Contains(trace, "err1") {
		t.Fail()
	}
}

func TestErrorWrapWithNew(t *testing.T) {
	err := WrapWithContextF(errors.New("err1"), "err2")
	err2 := Wrap(err)
	trace := SprintStackTraceNd(err2)
	println(trace)
	if !strings.Contains(trace, "err1") {
		t.Fail()
	}
	if !strings.Contains(trace, "[err2]") {
		t.Fail()
	}
}

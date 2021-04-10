package errore

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

func NewWithContext(format string, v ...interface{}) error {
	pc, file, line, _ := runtime.Caller(1)
	functionName := runtime.FuncForPC(pc).Name()
	err := fmt.Sprintf(format, v...)
	return fmt.Errorf("at %s(%s:%d) %w", functionName, file, line, errors.New(err))
}

func WrapWithContext(err error) error {
	pc, file, line, _ := runtime.Caller(1)
	functionName := runtime.FuncForPC(pc).Name()
	return fmt.Errorf("at %s(%s:%d) %w", functionName, file, line, err)
}

func RootCause(err error) error {
	rootErr := err
	for e := err; e != nil; e = errors.Unwrap(e) {
		rootErr = e
	}
	return rootErr
}

func UnwrapAll(err error) []error {
	errorList := make([]error, 0)
	for e := err; e != nil; e = errors.Unwrap(e) {
		errorList = append(errorList, e)
	}
	return errorList
}

func SprintTrace(err error) string {
	builder := strings.Builder{}
	trace := Trace(err)
	for _, line := range trace {
		builder.Write([]byte(line + "\n"))
	}
	return builder.String()
}

func Trace(err error) []string {
	all := UnwrapAll(err)
	errLine := make([]string, 0)
	lastErr := ""
	for _, err2 := range all {
		if lastErr == "" {
			lastErr = err2.Error()
			continue
		}
		currentErrLen := len([]byte(err2.Error()))
		lastErrLen := len([]byte(lastErr))
		bytes := []byte(lastErr)
		errLine = append(errLine, string(bytes[:(lastErrLen-currentErrLen)-1]))
		lastErr = err2.Error()
	}
	errLine = append(errLine, lastErr)
	for i, j := 0, len(errLine)-1; i < j; i, j = i+1, j-1 {
		errLine[i], errLine[j] = errLine[j], errLine[i]
	}
	return errLine
}

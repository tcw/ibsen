package test

import (
	"github.com/tcw/ibsen/manager"
	"testing"
)

func TestLogTopicsManager_Read(t *testing.T) {
	setUp()
	const tenMB = 1024 * 1024 * 10
	handler := manager.NewTopicHandler(afs, rootPath, "cars", tenMB)
	err := handler.Load()
	if err != nil {
		t.Error(err)
	}
}

package ext4

import (
	"io/ioutil"
	"os"
	"testing"
)

const tenMB = 1024 * 1024 * 10
const testTopic = "testTopic"

func createTestDir(t *testing.T) string {
	testDir, err := ioutil.TempDir("", "ibsenTest")
	t.Log("created test dir: ", testDir)
	if err != nil {
		t.Error(err)
	}
	return testDir
}

func TestLogStorage_Create(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, tenMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Failed()
	}
}

func TestLogStorage_Drop(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	storage, err := NewLogStorage(dir, tenMB)
	if err != nil {
		t.Error(err)
	}
	create, err := storage.Create(testTopic)
	if err != nil {
		t.Error(err)
	}
	if !create {
		t.Fail()
	}
	drop, err := storage.Drop(testTopic)
	if err != nil {
		t.Error(err)
	}
	if !drop {
		t.Fail()
	}
	directory := doesTopicExist(dir, "."+testTopic)
	if !directory {
		t.Error("Topic has not been moved to . file")
		t.Fail()
	}

}

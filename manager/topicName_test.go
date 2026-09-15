package manager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/tcw/ibsen/access/common"
)

func TestManager_rejectsTopicNamesOutsideTheirDirectory(t *testing.T) {
	for _, name := range []string{"../escaped", "nested/../../escaped", "a/b", "..", ".", ""} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			afs := newTestAfs(t)
			m := newTestManager(t, afs)
			entries := [][]byte{[]byte("x")}
			if err := m.Write(common.TopicName(name), &entries); !errors.Is(err, common.ErrInvalidTopicName) {
				t.Errorf("write to topic %q: err=%v, want ErrInvalidTopicName", name, err)
			}
			if _, err := readTopic(m, name); !errors.Is(err, common.ErrInvalidTopicName) {
				t.Errorf("read of topic %q: err=%v, want ErrInvalidTopicName", name, err)
			}
			if exists, _ := afs.Exists("escaped"); exists {
				t.Error("files were created outside the data directory")
			}
			if files, _ := afs.ReadDir("data"); len(files) != 0 {
				t.Errorf("files were created in the data directory: %v", files)
			}
		})
	}
}

func TestManager_acceptsOrdinaryTopicNames(t *testing.T) {
	m := newTestManager(t, newTestAfs(t))
	for _, name := range []string{"orders", "orders.v2", "my-topic_1", "with space", "ÆØÅ"} {
		writeTopic(t, m, name, 0, 3)
		if got, err := readTopic(m, name); err != nil || len(got) != 3 {
			t.Errorf("topic %q: read %d entries with err=%v, want 3", name, len(got), err)
		}
	}
}

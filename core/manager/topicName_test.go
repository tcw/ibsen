package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tcw/ibsen/core/domain"
)

func TestManager_rejectsTopicNamesOutsideTheirDirectory(t *testing.T) {
	for _, name := range []string{"../escaped", "nested/../../escaped", "a/b", "..", ".", ""} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			afs := newTestRoot(t)
			m := newTestManager(t, afs)
			entries := [][]byte{[]byte("x")}
			if err := m.Write(domain.TopicName(name), &entries); !errors.Is(err, domain.ErrInvalidTopicName) {
				t.Errorf("write to topic %q: err=%v, want ErrInvalidTopicName", name, err)
			}
			if _, err := readTopic(m, name); !errors.Is(err, domain.ErrInvalidTopicName) {
				t.Errorf("read of topic %q: err=%v, want ErrInvalidTopicName", name, err)
			}
			// nothing above the data directory, which is what these names try to reach
			if _, err := os.Stat(filepath.Join(afs.path, "..", "escaped")); err == nil {
				t.Error("files were created outside the data directory")
			}
			if files, _ := afs.ReadDir("."); len(files) != 0 {
				t.Errorf("files were created in the data directory: %v", files)
			}
		})
	}
}

func TestManager_acceptsOrdinaryTopicNames(t *testing.T) {
	m := newTestManager(t, newTestRoot(t))
	for _, name := range []string{"orders", "orders.v2", "my-topic_1", "with space", "ÆØÅ"} {
		writeTopic(t, m, name, 0, 3)
		if got, err := readTopic(m, name); err != nil || len(got) != 3 {
			t.Errorf("topic %q: read %d entries with err=%v, want 3", name, len(got), err)
		}
	}
}

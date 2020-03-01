package ext4

import "github.com/tcw/ibsen/logStorage"

type LogStorage struct {
}

var _ logStorage.LogStorage = LogStorage{} // Verify that T implements I.
//var _ logStorage.LogStorage = (*LogStorage{})(nil) // Verify that *T implements I.

func (e LogStorage) Create(topic logStorage.Topic) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Drop(topic logStorage.Topic) (bool, error) {
	panic("implement me")
}

func (e LogStorage) Write(topic logStorage.Topic, entry logStorage.Entry) (int, error) {
	panic("implement me")
}

func (e LogStorage) ReadFromBeginning(logChan chan *logStorage.LogEntry, topic logStorage.Topic) error {
	panic("implement me")
}

func (e LogStorage) ReadFromNotIncluding(logChan chan *logStorage.LogEntry, topic logStorage.Topic, offset logStorage.Offset) error {
	panic("implement me")
}

func (e LogStorage) ListTopics() ([]logStorage.Topic, error) {
	panic("implement me")
}

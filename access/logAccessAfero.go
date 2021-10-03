package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
)

type LogAccessAfero struct {
	Afs                 *afero.Afero
	IbsenRootPath       commons.IbsenRootPath
	MaxBlockSizeInBytes uint64
	TopicWriters        map[string]*Topic
}

func init() {

}

func createTopicWriters() {

}

func (l LogAccessAfero) Write(topic commons.Topic, entries commons.Entries) (commons.Offset, error) {
	panic("implement me")
}

func (l LogAccessAfero) Read(topic commons.Topic, offset commons.Offset, entries commons.NumberOfEntries) (commons.Entries, error) {
	panic("implement me")
}

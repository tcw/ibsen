package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"sync"
)

type LogAccessAfero struct {
	Afs                 *afero.Afero
	IbsenRootPath       commons.IbsenRootPath
	MaxBlockSizeInBytes commons.BlockSizeInBytes
	Topics              map[commons.Topic]*TopicHandler
}

func (l *LogAccessAfero) LoadTopicFromFilesystem() error {

	var loadedTopics = map[commons.Topic]*TopicHandler{}
	topics, err := ListAllTopics(l.Afs, l.IbsenRootPath)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for _, topic := range topics {
		t := TopicHandler{
			Afs:      l.Afs,
			mu:       &sync.Mutex{},
			Topic:    commons.Topic(topic),
			RootPath: l.IbsenRootPath,
			Blocks:   make([]commons.Offset, 0),
			TopicWriter: TopicWriter{
				Afs:   l.Afs,
				Topic: commons.Topic(topic),
			},
		}
		err = t.updateFromFileSystem()
		if err != nil {
			return errore.WrapWithContext(err)
		}
		blockFileName, err := t.currentBlock()
		blockSize, err := BlockSize(l.Afs, string(blockFileName))
		if err != nil {
			return errore.WrapWithContext(err)
		}
		offset, err := FindLastOffset(l.Afs, string(blockFileName)) //Todo: use index
		if err != nil {
			return errore.WrapWithContext(err)
		}
		t.TopicWriter.update(commons.Offset(offset), commons.BlockSizeInBytes(blockSize))
		loadedTopics[commons.Topic(topic)] = &t
	}
	l.Topics = loadedTopics
	return nil
}

func (l LogAccessAfero) Write(topic commons.Topic, entries commons.Entries) (commons.Offset, error) { //needs m
	return l.Topics[topic].Write(entries)
}

func (l LogAccessAfero) Read(topic commons.Topic, offset commons.Offset, entries commons.NumberOfEntries) (commons.Entries, error) {
	panic("implement me")
}

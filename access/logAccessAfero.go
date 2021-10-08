package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"sync"
	"time"
)

type LogAccessAfero struct {
	Afs                 *afero.Afero
	IbsenRootPath       IbsenRootPath
	MaxBlockSizeInBytes BlockSizeInBytes
	Topics              map[Topic]*TopicHandler
}

func (l *LogAccessAfero) LoadTopicFromFilesystem() error {

	var loadedTopics = map[Topic]*TopicHandler{}
	topics, err := ListAllTopics(l.Afs, l.IbsenRootPath)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for _, topic := range topics {
		t := TopicHandler{
			Afs:      l.Afs,
			mu:       &sync.Mutex{},
			Topic:    Topic(topic),
			RootPath: l.IbsenRootPath,
			Blocks:   make([]Offset, 0),
			TopicWriter: TopicWriter{
				Afs:   l.Afs,
				Topic: Topic(topic),
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
		t.TopicWriter.update(Offset(offset), BlockSizeInBytes(blockSize))
		loadedTopics[Topic(topic)] = &t
	}
	l.Topics = loadedTopics
	return nil
}

func (l LogAccessAfero) Write(topic Topic, entries Entries) (Offset, error) { //needs m
	return l.Topics[topic].Write(entries, l.MaxBlockSizeInBytes)
}

func (l LogAccessAfero) Read(params ReadParams) error {
	offset, err := l.Topics[params.Topic].Read(params)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	for start := time.Now(); time.Since(start) < params.TTL; {
		if l.Topics[params.Topic].TopicWriter.CurrentOffset > offset {
			next := params
			next.Offset = offset
			offset, err = l.Topics[params.Topic].Read(next)
		}
	}
	return nil
}

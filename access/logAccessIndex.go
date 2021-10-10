package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
)

type TopicIndexerWriter struct {
	Afs                      *afero.Afero
	Topic                    Topic
	currentLogFileName       string
	currentLogFileByteOffset int64
	headIndex                Index
	indexSparsity            uint32
}

func (t TopicIndexerWriter) createIndexArchive(logFileName string, indexFileName string) error {
	index, err := createArchiveIndex(t.Afs, logFileName, 0, t.indexSparsity)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	err = saveIndex(t.Afs, indexFileName, index)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (t *TopicIndexerWriter) createNewHeadIndex(logFileName string) error {
	index, logByteOffset, err := createHeadIndex(t.Afs, logFileName, 0, t.indexSparsity)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.currentLogFileByteOffset = 0
	t.currentLogFileName = logFileName
	t.currentLogFileByteOffset = logByteOffset
	t.headIndex = index
	return err
}

func (t *TopicIndexerWriter) updateHeadIndex() error {
	index, logByteOffset, err := createHeadIndex(t.Afs, t.currentLogFileName, t.currentLogFileByteOffset, t.indexSparsity)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.currentLogFileByteOffset = logByteOffset
	t.headIndex.addIndex(index)
	return err
}

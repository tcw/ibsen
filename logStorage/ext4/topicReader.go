package ext4

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/logStorage"
)

type TopicReader struct {
	afs               *afero.Afero
	blockManager      *BlockManager
	currentOffset     uint64
	currentBlockIndex uint
}

func NewTopicRead(afs *afero.Afero, manager *BlockManager) (*TopicReader, error) {
	return &TopicReader{
		afs:               afs,
		blockManager:      manager,
		currentOffset:     0,
		currentBlockIndex: 0,
	}, nil
}

func (t *TopicReader) ReadBatchFromOffsetNotIncluding(readBatchParam logStorage.ReadBatchParam) error {
	currentBlockIndex, err := t.blockManager.findBlockIndexContainingOffset(readBatchParam.Offset)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	file, err := OpenFileForRead(t.afs, blockFileName)
	err = ReadLogBlockFromOffsetNotIncluding(file, readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return t.readBatchFromBlock(readBatchParam, blockIndex+1)
}

func (t *TopicReader) readBatchFromBlock(readBatchParam logStorage.ReadBatchParam, block int) error {
	blockIndex := block
	var entriesBytes []logStorage.LogEntry
	for {
		filename, err := t.blockManager.GetBlockFilename(blockIndex)
		if err != nil && err != EndOfBlock {
			return errore.WrapWithContext(err)
		}
		if err == EndOfBlock && entriesBytes != nil {
			readBatchParam.Wg.Add(1)
			readBatchParam.LogChan <- &logStorage.LogEntryBatch{Entries: entriesBytes}
			return nil
		}
		if err == EndOfBlock {
			return nil
		}
		read, err := OpenFileForRead(t.afs, filename)
		if err != nil {
			errC := read.Close()
			if errC != nil {
				return errore.WrapWithContext(errC)
			}
			return errore.WrapWithContext(err)
		}
		partial, hasSent, err := ReadLogInBatchesToEnd(read, entriesBytes, readBatchParam.LogChan,
			readBatchParam.Wg, readBatchParam.BatchSize)

		if err != nil {
			return errore.WrapWithContext(err)
		}
		if hasSent {
			entriesBytes = nil
		}
		entriesBytes = append(entriesBytes, partial.Entries...)
		err = read.Close()
		if err != nil {
			return errore.WrapWithContext(err)
		}
		blockIndex = blockIndex + 1
	}
}

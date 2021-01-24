package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"io"
)

type TopicReader struct {
	afs               *afero.Afero
	blockManager      *BlockManager
	currentBlockIndex uint
	currentByteOffset int64
}

func NewTopicReader(afs *afero.Afero, manager *BlockManager) (*TopicReader, error) {
	return &TopicReader{
		afs:               afs,
		blockManager:      manager,
		currentBlockIndex: 0,
		currentByteOffset: 0,
	}, nil
}

func (t *TopicReader) ReadBatchFromOffsetNotIncluding(readBatchParam ReadBatchParam) error {

	currentBlockIndex, err := t.blockManager.FindBlockIndexContainingOffset(readBatchParam.Offset)
	if err == BlockNotFound {
		return BlockNotFound
	}
	if err != nil {
		return errore.WrapWithContext(err)
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	file, err := OpenFileForRead(t.afs, blockFileName)
	err = ReadLogFileFromOffsetNotIncluding(file, readBatchParam)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if !t.blockManager.HasNextBlock(blockIndex) {
		seekOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			err = errore.WrapWithContext(err)
		}
		t.currentByteOffset = seekOffset
	} else {
		blockIndex, err = t.readBlocksInBatchesToTail(readBatchParam, blockIndex+1)
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	t.currentBlockIndex = uint(blockIndex)
	return nil
}

func (t *TopicReader) readBlocksInBatchesToTail(readBatchParam ReadBatchParam, block int) (int, error) {
	blockIndex := block
	var entriesBytes []LogEntry
	for {
		filename, err := t.blockManager.GetBlockFilename(blockIndex)
		if err == BlockNotFound {
			if entriesBytes != nil {
				readBatchParam.Wg.Add(1)
				readBatchParam.LogChan <- &LogEntryBatch{Entries: entriesBytes}
			}
			//TODO: add positional information
			return blockIndex - 1, nil
		}
		if err != nil {
			return blockIndex, errore.WrapWithContext(err)
		}
		file, err := OpenFileForRead(t.afs, filename)
		if err != nil {
			errC := file.Close()
			if errC != nil {
				err = errore.WrapWithContext(errC)
			}
			return blockIndex, errore.WrapWithContext(err)
		}
		logEntryBatch, hasSent, err := ReadLogFileInBatches(file, entriesBytes, readBatchParam.LogChan,
			readBatchParam.Wg, readBatchParam.BatchSize)
		if !t.blockManager.HasNextBlock(blockIndex) {
			seekOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				err = errore.WrapWithContext(err)
			}
			t.currentByteOffset = seekOffset
		}
		if err != nil {
			errC := file.Close()
			if errC != nil {
				err = errore.WrapWithContext(errC)
			}
			return blockIndex, errore.WrapWithContext(err)
		}
		if hasSent {
			entriesBytes = nil
		}
		entriesBytes = append(entriesBytes, logEntryBatch.Entries...)
		err = file.Close()
		if err != nil {
			return blockIndex, errore.WrapWithContext(err)
		}
		if t.blockManager.HasNextBlock(blockIndex) {
			blockIndex = blockIndex + 1
		} else {
			return blockIndex, nil
		}
	}
}

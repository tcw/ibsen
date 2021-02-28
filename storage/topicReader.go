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
	currentOffset     uint64
	currentByteOffset int64
}

func NewTopicReader(afs *afero.Afero, manager *BlockManager) (*TopicReader, error) {
	return &TopicReader{
		afs:               afs,
		blockManager:      manager,
		currentBlockIndex: 0,
		currentOffset:     0,
		currentByteOffset: 0,
	}, nil
}

func (t *TopicReader) ReadFromOffset(readBatchParam ReadBatchParam) (ReadBatchParam, error) {

	currentBlockIndex, err := t.blockManager.FindBlockIndexContainingOffset(readBatchParam.Offset)
	if err == BlockNotFound {
		return ReadBatchParam{}, BlockNotFound
	}
	if err != nil {
		return ReadBatchParam{}, errore.WrapWithContext(err)
	}
	blockIndex := int(currentBlockIndex)
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return ReadBatchParam{}, errore.WrapWithContext(err)
	}
	file, err := OpenFileForRead(t.afs, blockFileName)
	err = ReadFileFromLogOffset(file, readBatchParam)
	if err != nil {
		return ReadBatchParam{}, errore.WrapWithContext(err)
	}
	if !t.blockManager.HasNextBlock(blockIndex) {
		seekOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			err = errore.WrapWithContext(err)
		}
		t.currentByteOffset = seekOffset
	} else {
		blockIndex, err = t.readBlocks(readBatchParam, blockIndex+1)
		if err != nil {
			return ReadBatchParam{}, errore.WrapWithContext(err)
		}
	}
	t.currentBlockIndex = uint(blockIndex)
	return ,nil
}

func (t *TopicReader) readBlocks(readBatchParam ReadBatchParam, block int) (int, error) {
	blockIndex := block

	for {
		err := t.readBlock(readBatchParam, blockIndex)
		if err == BlockNotFound {
			return i, nil // Todo: continue here!!
		}

		if !t.blockManager.HasNextBlock(blockIndex) {
			seekOffset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				err = errore.WrapWithContext(err)
			}
			t.currentByteOffset = seekOffset
		}

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

func (t *TopicReader) readBlock(readBatchParam ReadBatchParam, blockIndex int) error {
	filename, err := t.blockManager.GetBlockFilename(blockIndex)
	if err == BlockNotFound {
		return err
	}
	if err != nil {
		return errore.WrapWithContext(err)
	}
	file, err := OpenFileForRead(t.afs, filename)
	if err != nil {
		errC := file.Close()
		if errC != nil {
			err = errore.WrapWithContext(errC)
		}
		return  errore.WrapWithContext(err)
	}
	err = ReadFile(file, readBatchParam.LogChan,
		readBatchParam.Wg, readBatchParam.BatchSize)
	if err != nil {
		errC := file.Close()
		if errC != nil {
			err = errore.WrapWithContext(errC)
		}
		return errore.WrapWithContext(err)
	}
	return nil
}

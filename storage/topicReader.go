package storage

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/commons"
	"github.com/tcw/ibsen/errore"
	"io"
)

type TopicReader struct {
	afs          *afero.Afero
	blockManager *BlockManager
}

func NewTopicReader(afs *afero.Afero, manager *BlockManager) (*TopicReader, error) {
	return &TopicReader{
		afs:          afs,
		blockManager: manager,
	}, nil
}

func (t *TopicReader) ReadFromInternalOffset(readBatchParam ReadBatchParam, blockIndex int, internalOffset int64) (int, int64, error) {
	return t.readFrom(readBatchParam, blockIndex, internalOffset)
}

func (t *TopicReader) ReadFromOffset(readBatchParam ReadBatchParam) (int, int64, error) {
	currentBlockIndex, err := t.blockManager.FindBlockIndexContainingOffset(readBatchParam.Offset)
	if err == commons.BlockNotFound {
		return 0, 0, commons.BlockNotFound
	}
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	return t.readFrom(readBatchParam, int(currentBlockIndex), 0)

}

func (t *TopicReader) readFrom(readBatchParam ReadBatchParam, blockIndex int, internalOffset int64) (int, int64, error) {
	blockFileName, err := t.blockManager.GetBlockFilename(blockIndex)
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	file, err := commons.OpenFileForRead(t.afs, blockFileName)
	if internalOffset == 0 {
		err = ReadFileFromLogOffset(file, readBatchParam)
	} else {
		err = ReadFileFromLogInternalOffset(file, readBatchParam, internalOffset)
	}
	if err != nil {
		return 0, 0, errore.WrapWithContext(err)
	}
	var seekOffset int64
	if t.blockManager.HasNextBlock(blockIndex) {
		blockIndex, err = t.readBlocksFrom(readBatchParam, blockIndex+1)
		if err != nil {
			return 0, 0, errore.WrapWithContext(err)
		}
	} else {
		seekOffset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			err = errore.WrapWithContext(err)
		}
	}
	return blockIndex, seekOffset, nil
}

func (t *TopicReader) readBlocksFrom(readBatchParam ReadBatchParam, block int) (int, error) {
	blockIndex := block

	for {
		err := t.readBlock(readBatchParam, blockIndex)
		if err == commons.BlockNotFound {
			return 0, nil
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
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
	if err == commons.BlockNotFound {
		return err
	}
	if err != nil {
		return errore.WrapWithContext(err)
	}
	file, err := commons.OpenFileForRead(t.afs, filename)
	if err != nil {
		errC := file.Close()
		if errC != nil {
			err = errore.WrapWithContext(errC)
		}
		return errore.WrapWithContext(err)
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
	err = file.Close()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

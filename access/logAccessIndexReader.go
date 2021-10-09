package access

import (
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/errore"
	"io"
)

func loadIndex(afs *afero.Afero, indexFileName string) (StrictOrderIndex, error) {
	file, err := OpenFileForRead(afs, indexFileName)
	defer file.Close()
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, errore.WrapWithContext(err)
	}
	return bytes, nil
}

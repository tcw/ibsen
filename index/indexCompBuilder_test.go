package index

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/storage"
	"testing"
)

func newAfero() *afero.Afero {
	var fs = afero.NewMemMapFs()
	return &afero.Afero{Fs: fs}
}

func Test_writeToFile(t *testing.T) {
	afs := newAfero()
	file, err := afs.Create("00000000000000000000.index")
	if err != nil {
		t.Error(err)
	}
	err = writeToFile(
		file,
		storage.OffsetPosition{
			Offset:     0,
			ByteOffset: 0,
		},
		[]storage.OffsetPosition{{
			Offset:     1,
			ByteOffset: 100,
		}, {
			Offset:     2,
			ByteOffset: 200,
		}})
	if err != nil {
		t.Error(err)
	}
	err = file.Close()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(file.Name())
	offsetMap, err := readFromFile(afs, file.Name())
	if err != nil {
		t.Error(err)
	}
	fmt.Print(offsetMap)

}

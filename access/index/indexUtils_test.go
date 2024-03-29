package index

import (
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access/common"
	"strconv"
	"testing"
)

func TestCreateIndex(t *testing.T) {
	var fs = afero.NewMemMapFs()
	afs := &afero.Afero{Fs: fs}
	err := afs.Mkdir("tmp", 0744)
	assert.Nil(t, err)
	err = afs.WriteFile("tmp/test.log", createLogEntries(10), 0744)
	assert.Nil(t, err)
	indexBytes, _, err := CreateBinaryIndexFromLogFile(afs, "tmp/test.log", 0, 1)
	assert.Nil(t, err)
	index := NewIndex(indexBytes)
	assert.Nil(t, err)
	assert.Equal(t, index.Size(), 9)
}

func createLogEntries(entries int) []byte {
	var log = make([]byte, 0)
	for i := 0; i < entries; i++ {
		log = append(log, common.CreateByteEntry([]byte("dummy"+strconv.Itoa(i)), common.Offset(i))...)
	}
	return log
}

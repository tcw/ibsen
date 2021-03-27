package index

import (
	"fmt"
	"testing"
)

func Test_writeModToFile(t *testing.T) {
	afs := newAfero()
	file, err := afs.Create("00000000000000000000_5.index")
	if err != nil {
		t.Error(err)
	}
	lastByteOffset, err := WriteByteOffsetToFile(
		file,
		0,
		[]ByteOffset{100, 200})
	if err != nil {
		t.Error(err)
	}
	if lastByteOffset != 200 {
		t.Fail()
	}
	err = file.Close()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(file.Name())
	offsetMap, err := ReadByteOffsetFromFile(afs, file.Name())
	if err != nil {
		t.Error(err)
	}
	fmt.Print(offsetMap)

}

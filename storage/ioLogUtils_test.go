package storage

import (
	"github.com/spf13/afero"
	"os"
	"reflect"
	"testing"
)

func TestListFilesInDirectory(t *testing.T) {

	var fs = afero.NewMemMapFs()
	afs := &afero.Afero{Fs: fs}
	afs.Mkdir("dummyDir", os.FileMode(777))
	afs.Create("dummyDir/dummy1.log")
	afs.Create("dummyDir/dummy2.log")
	afs.Create("dummyDir/dummy3.index")

	type args struct {
		afs      *afero.Afero
		dir      string
		filetype string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"List .log files", args{afs: afs, dir: "dummyDir", filetype: "log"}, []string{"dummy1.log", "dummy2.log"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListFilesInDirectory(tt.args.afs, tt.args.dir, tt.args.filetype)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListFilesInDirectory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListFilesInDirectory() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenFileForRead(t *testing.T) {
	type args struct {
		afs      *afero.Afero
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    afero.File
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenFileForRead(tt.args.afs, tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFileForRead() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OpenFileForRead() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenFileForReadWrite(t *testing.T) {
	type args struct {
		afs      *afero.Afero
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    afero.File
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenFileForReadWrite(tt.args.afs, tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFileForReadWrite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OpenFileForReadWrite() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenFileForWrite(t *testing.T) {
	type args struct {
		afs      *afero.Afero
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    afero.File
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenFileForWrite(tt.args.afs, tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFileForWrite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OpenFileForWrite() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTopicBlocks_isEmpty(t *testing.T) {
	type fields struct {
		Topic  string
		Blocks []int64
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &TopicBlocks{
				Topic:  tt.fields.Topic,
				Blocks: tt.fields.Blocks,
			}
			if got := tb.isEmpty(); got != tt.want {
				t.Errorf("isEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_intToLittleEndian(t *testing.T) {
	type args struct {
		number int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intToLittleEndian(tt.args.number); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intToLittleEndian() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ListUnhiddenEntriesDirectory(t *testing.T) {

	var fs = afero.NewMemMapFs()
	afs := &afero.Afero{Fs: fs}
	afs.Mkdir("dummy", os.FileMode(777))
	afs.Mkdir("dummy/dir1", os.FileMode(777))
	afs.Mkdir("dummy/dir2", os.FileMode(777))
	afs.Mkdir("dummy/.dirHidden", os.FileMode(777))

	type args struct {
		afs  *afero.Afero
		root string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"List 2 unhidden directories", args{afs: afs, root: "dummy"}, []string{"dir1", "dir2"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListUnhiddenEntriesDirectory(tt.args.afs, tt.args.root)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListUnhiddenEntriesDirectory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListUnhiddenEntriesDirectory() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_littleEndianToUint32(t *testing.T) {
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := littleEndianToUint32(tt.args.bytes); got != tt.want {
				t.Errorf("littleEndianToUint32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_littleEndianToUint64(t *testing.T) {
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := littleEndianToUint64(tt.args.bytes); got != tt.want {
				t.Errorf("littleEndianToUint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_uint32ToLittleEndian(t *testing.T) {
	type args struct {
		number uint32
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uint32ToLittleEndian(tt.args.number); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uint32ToLittleEndian() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_uint64ToLittleEndian(t *testing.T) {
	type args struct {
		offset uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uint64ToLittleEndian(tt.args.offset); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uint64ToLittleEndian() = %v, want %v", got, tt.want)
			}
		})
	}
}

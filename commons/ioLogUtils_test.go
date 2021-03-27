package commons

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

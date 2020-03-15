package ext4

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLogStorage_Create(t *testing.T) {
	type fields struct {
		rootPath     string
		maxBlockSize int64
	}
	type args struct {
		topic string
	}
	testDir, err := ioutil.TempDir("", "ibsenTest")
	t.Log("created test dir: ", testDir)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(testDir)

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{"CreateTopic",
			fields{
				rootPath:     testDir,
				maxBlockSize: 1024 * 1024 * 10,
			},
			args{topic: "unittest"},
			true,
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, err2 := NewLogStorage(tt.fields.rootPath, tt.fields.maxBlockSize)
			if err2 != nil {
				t.Errorf("Failed on init")
			}
			got, err := storage.Create(tt.args.topic)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Create() got = %v, want %v", got, tt.want)
			}
		})
	}
}

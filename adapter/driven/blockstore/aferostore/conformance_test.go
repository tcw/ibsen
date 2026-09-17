package aferostore

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driven/blockstore/conformance"
	"github.com/tcw/ibsen/core/port/driven"
)

// TestAferoStore_Conformance runs the shared suite over both filesystems the adapter is
// used with: the in-memory one tests reach for, and a real directory on disk.
func TestAferoStore_Conformance(t *testing.T) {
	t.Run("mem", func(t *testing.T) {
		conformance.Run(t, func(t *testing.T) driven.BlockStore {
			store, _ := NewMem("data")
			return store
		})
	})
	t.Run("os", func(t *testing.T) {
		conformance.Run(t, func(t *testing.T) driven.BlockStore {
			return New(&afero.Afero{Fs: afero.NewOsFs()}, t.TempDir())
		})
	})
}

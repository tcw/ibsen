package filestore

import (
	"testing"

	"github.com/tcw/ibsen/adapter/driven/blockstore/conformance"
	"github.com/tcw/ibsen/core/port/driven"
)

// TestFileStore_Conformance runs the shared suite against a real directory, which is the only
// filesystem this adapter has. The adapter it replaces ran the suite twice, once against an
// emulated filesystem, and that second run is what this one deliberately does not do: an
// emulation that disagrees with a real filesystem is worse than no second run, and this one
// disagreed twice.
func TestFileStore_Conformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driven.BlockStore {
		return NewOS(t.TempDir())
	})
}

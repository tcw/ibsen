//go:build !(linux && (amd64 || arm64 || arm))

package filestore

// fadviseDontNeed does nothing where the build has no posix_fadvise to ask: the page cache is
// left as it is, which is what every build did before it asked.
func fadviseDontNeed(uintptr) error { return nil }

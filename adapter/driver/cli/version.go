package cli

import rtdebug "runtime/debug"

// Version is the release this binary was built from. A release build stamps it with
//
//	-ldflags "-X github.com/tcw/ibsen/adapter/driver/cli.Version=v1.2.3"
//
// which is what .goreleaser.yaml does. Left empty, it is read from the module the binary was
// built as, so `go install github.com/tcw/ibsen@v1.2.3` reports v1.2.3 too, and a build from a
// working tree reports "dev".
var Version = ""

// version is what `ibsen --version` prints.
func version() string {
	if Version != "" {
		return Version
	}
	if info, ok := rtdebug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}

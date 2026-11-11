// ibsen-k8s is the ibsen binary with one more single-writer lock: --lock k8s, held in a
// Kubernetes Lease object. It is a separate entry point because the Kubernetes client is a heavy
// dependency the ibsen binary does not carry; everything else — the server, the flags, the
// filter commands — is the same. See docs/OPERATIONS.md#running-on-kubernetes.
package main

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tcw/ibsen/adapter/driver/cli"
	"github.com/tcw/ibsen/wiring/k8s"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.TimestampFieldName = "t"
	zerolog.LevelFieldName = "l"
	zerolog.MessageFieldName = "m"
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	cli.ExtendServer(k8s.Register)
	cli.Execute()
}

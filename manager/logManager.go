package manager

import (
	"github.com/tcw/ibsen/access"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (access.Offset, error)
	Read(params access.ReadParams) error
}

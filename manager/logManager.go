package manager

import (
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/access/data"
)

type LogManager interface {
	Write(topic access.Topic, entries access.Entries) (access.Offset, error)
	Read(params data.ReadParams) error
}

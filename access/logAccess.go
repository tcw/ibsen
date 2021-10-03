package access

import "github.com/tcw/ibsen/commons"

type LogAccess interface {
	Write(topic commons.Topic, entries commons.Entries) (commons.Offset, error)
	Read(topic commons.Topic, offset commons.Offset, entries commons.NumberOfEntries) (commons.Entries, error)
}

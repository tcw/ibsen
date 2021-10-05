package access

type LogAccess interface {
	Write(topic Topic, entries Entries) (Offset, error)
	Read(topic Topic, offset Offset, entries NumberOfEntries) (Entries, error)
}

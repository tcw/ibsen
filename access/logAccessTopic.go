package access

import "github.com/tcw/ibsen/commons"

type Topic struct {
	Topic       string
	rootPath    commons.IbsenRootPath
	Blocks      []uint64
	TopicWriter TopicWriter
}

func (t *Topic) updateFromFileSystem() {

}

func (t *Topic) createNewBlock(offset commons.Offset) commons.FileName {
	return ""
}

func (t *Topic) currentBlock() commons.FileName {
	return ""
}

func (t Topic) findBlockContaining(offset commons.Offset) commons.FileName {

}

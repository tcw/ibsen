package manager

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/errore"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var NoOffset error = errors.New("no offset exits for this topic")

type TopicHandler struct {
	Afs            *afero.Afero
	ReadOnly       bool
	RootPath       string
	Topic          access.Topic
	MaxBlockSize   access.BlockSizeInBytes
	HeadBlockSize  access.BlockSizeInBytes
	LogBlocks      *access.Blocks
	NextLogOffset  access.Offset
	writeLock      sync.Mutex
	loadLock       sync.Mutex
	IndexBlocks    *access.Blocks
	IndexOffset    access.Offset
	indexMutex     int32
	LogAccess      access.LogAccess
	LogIndexAccess access.LogIndexAccess
	loaded         bool
}

func NewTopicHandler(afs *afero.Afero, readOnly bool, rootPath string, topic access.Topic, maxBlockSize uint64) TopicHandler {
	return TopicHandler{
		Afs:           afs,
		ReadOnly:      readOnly,
		RootPath:      rootPath,
		MaxBlockSize:  access.BlockSizeInBytes(maxBlockSize),
		Topic:         topic,
		NextLogOffset: 0,
		writeLock:     sync.Mutex{},
		IndexOffset:   0,
		indexMutex:    0,
		LogAccess: access.ReadWriteLogAccess{
			Afs:      afs,
			RootPath: rootPath,
		},
		LogIndexAccess: access.ReadWriteLogIndexAccess{
			Afs:          afs,
			RootPath:     rootPath,
			IndexDensity: 0.01,
		},
		loaded: false,
	}
}

func (t *TopicHandler) Write(entries access.Entries) (uint32, error) {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()
	err := t.Load()
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	if t.LogBlocks.IsEmpty() {
		t.LogBlocks.AddBlock(0)
	}
	head := t.LogBlocks.Head()
	offset, bytes, err := t.LogAccess.Write(head.LogFileName(t.RootPath, t.Topic), entries, t.NextLogOffset)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	t.NextLogOffset = offset
	t.HeadBlockSize = t.HeadBlockSize + bytes
	if t.HeadBlockSize > t.MaxBlockSize {
		t.LogBlocks.AddBlock(access.Block(t.NextLogOffset))
		t.HeadBlockSize = 0
	}
	go t.updateIndex()
	return uint32(bytes), nil
}

func (t *TopicHandler) Read(params access.ReadParams) (access.Offset, error) {
	err := t.Load()
	if params.Offset == t.NextLogOffset {
		return params.Offset, nil
	}
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	blocks, err := t.LogBlocks.GetBlocksIncludingAndAfter(params.Offset)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	if len(blocks) == 0 {
		return 0, errore.WrapWithContext(errors.New("no blocks"))
	}
	var byteOffset int64 = 0
	if params.Offset > 0 {
		byteOffset, err = t.lookUpIndexedOffset(params.Offset)
		if err != nil && err != access.IndexEntryNotFound {
			return 0, errore.WrapWithContext(err)
		}
	}
	var lastReadOffset = params.Offset
	for _, block := range blocks {
		logFileName := block.LogFileName(t.RootPath, t.Topic)
		exists, err := t.Afs.Exists(string(logFileName))
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		if !exists {
			return lastReadOffset, nil
		}
		currentOffset, err := t.currentOffset()
		if err == NoOffset {
			return 0, err
		}
		lastReadOffset, err = t.LogAccess.Read(logFileName, params, byteOffset, currentOffset)
		if err == io.EOF {
			return lastReadOffset, nil
		}
		if err != nil {
			return 0, errore.WrapWithContext(err)
		}
		byteOffset = 0
	}
	return lastReadOffset, nil
}

func (t *TopicHandler) Load() error {
	if !t.loaded {
		t.loadLock.Lock()
		defer t.loadLock.Unlock()
		if !t.loaded {
			err := t.lazyLoad()
			if err != nil {
				return errore.WrapWithContext(err)
			}
			t.loaded = true
			if !t.ReadOnly {
				go t.indexScheduler()
			}
		}
	}
	return nil
}

func (t *TopicHandler) Status() (string, error) {
	logBlocks := t.LogBlocks
	indexBlocks := t.IndexBlocks
	logOffset := t.NextLogOffset
	indexOffset := t.IndexOffset
	topic := t.Topic
	inTopic, err := access.ListAllFilesInTopic(t.Afs, t.RootPath, t.Topic)
	if err != nil {
		return "", errore.WrapWithContext(err)
	}
	report := "\n"
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("Topic: %s\n", topic)
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("Next Log Offset: %d\n", logOffset)
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("Index Log Offset: %d\n", indexOffset)
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("Log Blocks:\n")
	report = report + logBlocks.ToString()
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("Index Log Blocks:\n")
	report = report + indexBlocks.ToString()
	report = report + fmt.Sprintf("----------------------------------------------------\n")
	report = report + fmt.Sprintf("On Disk: \n")
	for _, info := range inTopic {
		report = report + fmt.Sprintf("File name: %s Size:%d\n", info.Name(), info.Size())
	}
	report = report + fmt.Sprintf("----------------------------------------------------\n")

	return report, nil
}

func (t *TopicHandler) lazyLoad() error {
	topicPath := t.RootPath + access.Sep + string(t.Topic)
	exists, err := t.Afs.Exists(topicPath)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if !t.ReadOnly && !exists {
		err = t.Afs.Mkdir(topicPath, 0770) //Todo: do more restrictive
		if err != nil {
			return errore.WrapWithContext(err)
		}
	}
	blocks, err := t.LogAccess.ReadTopicLogBlocks(t.Topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.LogBlocks = &blocks
	indexBlocks, err := t.LogIndexAccess.ReadTopicIndexBlocks(t.Topic)
	if err != nil {
		return errore.WrapWithContext(err)
	}
	t.IndexBlocks = &indexBlocks
	offset, err := t.findLastOffset()
	log.Printf("Loading topic %s, current offset %d", t.Topic, offset)
	head := t.LogBlocks.Head()
	logFileName := head.LogFileName(t.RootPath, t.Topic)
	fileExists, err := t.Afs.Exists(string(logFileName))
	if err != nil {
		return errore.WrapWithContext(err)
	}
	if fileExists {
		size, err := access.FileSize(t.Afs, string(logFileName))
		if err != nil {
			return errore.WrapWithContext(err)
		}
		t.HeadBlockSize = access.BlockSizeInBytes(size)
	}

	t.NextLogOffset = offset
	return nil
}

func (t *TopicHandler) indexScheduler() {
	for {
		err := t.updateIndex()
		if err != nil {
			log.Printf("index builder for topic %s has failed: %s", t.Topic,
				errore.SprintTrace(errore.WrapWithContext(err)))
		}
		time.Sleep(time.Second * 10)
	}
}

func (t *TopicHandler) updateIndex() error {
	if !atomic.CompareAndSwapInt32(&t.indexMutex, 0, 1) {
		return nil
	}
	defer atomic.CompareAndSwapInt32(&t.indexMutex, 1, 0)

	if t.LogBlocks.IsEmpty() {
		return nil
	}

	var byteOffset int64 = 0
	if t.IndexBlocks.IsEmpty() {
		t.IndexBlocks.AddBlock(0)
	} else {
		head := t.IndexBlocks.Head()
		index, err := t.LogIndexAccess.Read(head.IndexFileName(t.RootPath, t.Topic))
		if err != nil {
			return errore.WrapWithContext(err)
		}
		byteOffset = index.Head().ByteOffset
	}
	logBlocks := *t.LogBlocks

	lastIndexBlock := t.IndexBlocks.Size() - 1
	lastLogBlock := logBlocks.Size() - 1
	for i := lastIndexBlock; i < logBlocks.Size(); i++ {
		logBlock := logBlocks.Get(i)
		_, err := t.LogIndexAccess.Write(logBlock.LogFileName(t.RootPath, t.Topic), byteOffset)
		if err == access.NoFile {
			return nil
		}
		if err != nil {
			return errore.WrapWithContext(err)
		}
		if i != lastLogBlock {
			t.IndexBlocks.AddBlock(logBlocks.Get(i + 1))
		}
	}

	return nil
}

func (t *TopicHandler) lookUpIndexedOffset(offset access.Offset) (int64, error) {
	block, err := t.IndexBlocks.Contains(offset)
	if err == access.BlockNotFound {
		return 0, access.IndexEntryNotFound
	}
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	logFileName := block.LogFileName(t.RootPath, t.Topic)
	indexFileName := block.IndexFileName(t.RootPath, t.Topic)
	index, err := t.LogIndexAccess.Read(indexFileName)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	byteOffset := index.FindNearestByteOffset(offset)
	fromOffset, err := access.FindByteOffsetFromOffset(t.Afs, logFileName, byteOffset, offset)
	if err == io.EOF {
		return 0, access.IndexEntryNotFound
	}
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return fromOffset, nil
}

func (t *TopicHandler) findLastOffset() (access.Offset, error) {
	logHead := t.LogBlocks.Head()
	indexFileName := logHead.IndexFileName(t.RootPath, t.Topic)
	exists, err := t.Afs.Exists(string(indexFileName))
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	if !exists {
		return 0, nil
	}
	index, err := t.LogIndexAccess.Read(indexFileName)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	indexHead := index.Head()
	byteOffset := indexHead.ByteOffset
	offset, err := access.FindLastOffset(t.Afs, logHead.LogFileName(t.RootPath, t.Topic), byteOffset)
	if err != nil {
		return 0, errore.WrapWithContext(err)
	}
	return access.Offset(offset), nil
}

func (t *TopicHandler) currentOffset() (access.Offset, error) {
	if t.NextLogOffset == 0 {
		return 0, NoOffset
	}
	return t.NextLogOffset - access.Offset(1), nil
}

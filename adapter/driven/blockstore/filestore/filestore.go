// Package filestore is the filesystem adapter behind the driven.BlockStore port: every topic
// is a directory of numbered block files, reached through the standard library and nothing
// else.
//
// Layout, unchanged from the adapter it replaces, is <root>/<topic>/%020d.log and .idx, so a
// data directory written by either is read by either.
package filestore

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/errore"
)

const (
	topicPerm = os.FileMode(0744)
	blockPerm = os.FileMode(0600)
)

// Store keeps blocks as files under a root directory.
type Store struct {
	fs       FS
	rootPath string

	// mu guards newBlocks, which is the set of blocks whose file has been created but whose
	// directory entry has not been synced yet, and newTopics, the same for topic directories
	// and the root. Appends and syncs of different topics run concurrently, so the sets need
	// a lock of their own.
	mu        sync.Mutex
	newBlocks map[driven.BlockRef]struct{}
	newTopics map[domain.TopicName]struct{}
}

var (
	_ driven.BlockStore = &Store{}
	_ driven.Syncable   = &Store{}
)

// New returns a store that keeps its topics under rootPath, which must exist, on the given
// filesystem. Pass OS{} unless you are injecting faults.
func New(fs FS, rootPath string) *Store {
	return &Store{
		fs: fs, rootPath: rootPath,
		newBlocks: make(map[driven.BlockRef]struct{}),
		newTopics: make(map[domain.TopicName]struct{}),
	}
}

// NewOS returns a store on the real filesystem, which is what the server wires.
func NewOS(rootPath string) *Store {
	return New(OS{}, rootPath)
}

// RootPath is the directory the store keeps its topics under.
func (s *Store) RootPath() string {
	return s.rootPath
}

func (s *Store) topicPath(topic domain.TopicName) string {
	return filepath.Join(s.rootPath, string(topic))
}

func (s *Store) blockPath(ref driven.BlockRef) string {
	return filepath.Join(s.topicPath(ref.Topic), BlockFileName(ref))
}

// BlockFileName is the name this adapter keeps a block under. It is exported so a test can
// look at the bytes on disk without going through the port.
func BlockFileName(ref driven.BlockRef) string {
	name := strconv.FormatUint(ref.Block, 10)
	return strings.Repeat("0", 20-len(name)) + name + extension(ref.Kind)
}

func extension(kind driven.BlockKind) string {
	if kind == driven.Index {
		return ".idx"
	}
	return ".log"
}

// isDir reports whether a path is a directory that exists.
func (s *Store) isDir(path string) (bool, error) {
	info, err := s.fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (s *Store) exists(path string) (bool, error) {
	_, err := s.fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) Topics() ([]domain.TopicName, error) {
	infos, err := s.fs.ReadDir(s.rootPath)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	var topics []domain.TopicName
	for _, info := range infos {
		// topics are directories; hidden entries and stray files are not topics
		if info.IsDir() && !strings.HasPrefix(info.Name(), ".") {
			topics = append(topics, domain.TopicName(info.Name()))
		}
	}
	return topics, nil
}

func (s *Store) CreateTopic(topic domain.TopicName) (bool, error) {
	path := s.topicPath(topic)
	exists, err := s.isDir(path)
	if err != nil {
		return false, errore.Wrap(err)
	}
	if exists {
		return false, nil
	}
	if err = s.fs.Mkdir(path, topicPerm); err != nil {
		// another caller may have created it after the check
		if exists, _ := s.isDir(path); exists {
			return false, nil
		}
		return false, errore.Wrap(err)
	}
	s.markNewTopic(topic)
	return true, nil
}

func (s *Store) List(topic domain.TopicName, kind driven.BlockKind) ([]driven.Block, error) {
	infos, err := s.fs.ReadDir(s.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errore.Wrap(err)
	}
	want := extension(kind)
	var blocks []driven.Block
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		block, ext, ok := parseBlockFileName(info.Name())
		if !ok {
			// a stray file is not ours to interpret, and not ours to delete either
			continue
		}
		if ext != want {
			continue
		}
		blocks = append(blocks, driven.Block{Block: block, Size: info.Size()})
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Block < blocks[j].Block })
	return blocks, nil
}

// StrayFiles lists the names in a topic directory that are not block files, so wiring can
// report them. The port itself ignores them.
func (s *Store) StrayFiles(topic domain.TopicName) ([]string, error) {
	infos, err := s.fs.ReadDir(s.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errore.Wrap(err)
	}
	var stray []string
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		if _, _, ok := parseBlockFileName(info.Name()); !ok {
			stray = append(stray, info.Name())
		}
	}
	return stray, nil
}

// parseBlockFileName parses a block file name as the store writes it, such as
// 00000000000000000042.log. ok is false for any other name.
func parseBlockFileName(name string) (block uint64, extension string, ok bool) {
	extension = filepath.Ext(name)
	if extension != ".log" && extension != ".idx" {
		return 0, "", false
	}
	digits := strings.TrimSuffix(name, extension)
	if len(digits) != 20 {
		return 0, "", false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, "", false
		}
	}
	block, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return block, extension, true
}

func (s *Store) Append(ref driven.BlockRef, data []byte) (driven.Block, error) {
	file, err := s.openForAppend(ref)
	if err != nil {
		return driven.Block{}, errore.Wrap(err)
	}
	info, err := file.Stat()
	if err != nil {
		closeQuietly(file)
		return driven.Block{}, errore.Wrap(err)
	}
	size := info.Size()
	n, err := file.Write(data)
	if err != nil {
		// leave the block as it was, so the next append does not follow a partial one
		if truncErr := file.Truncate(size); truncErr != nil {
			closeQuietly(file)
			return driven.Block{}, driven.DirtyBlock(errore.WrapError(truncErr, err))
		}
		closeQuietly(file)
		return driven.Block{}, errore.Wrap(err)
	}
	if err = file.Close(); err != nil {
		return driven.Block{}, errore.Wrap(err)
	}
	if size == 0 {
		// the block was empty, so this append either created the file or filled a file
		// nothing had been made durable from: its directory entry still has to be synced.
		// An existing empty block costs one directory sync it does not need, which is the
		// price of not asking the filesystem whether the open created the file.
		s.markNewBlock(ref)
	}
	return driven.Block{Block: ref.Block, Size: size + int64(n)}, nil
}

// markNewBlock records that ref's directory entry is not on durable media yet.
func (s *Store) markNewBlock(ref driven.BlockRef) {
	s.mu.Lock()
	s.newBlocks[ref] = struct{}{}
	s.mu.Unlock()
}

// blockIsNew reports whether ref's directory entry still has to be synced.
func (s *Store) blockIsNew(ref driven.BlockRef) bool {
	s.mu.Lock()
	_, isNew := s.newBlocks[ref]
	s.mu.Unlock()
	return isNew
}

// markNewTopic records that a topic directory's name in the root is not on durable media yet.
func (s *Store) markNewTopic(topic domain.TopicName) {
	s.mu.Lock()
	s.newTopics[topic] = struct{}{}
	s.mu.Unlock()
}

// topicIsNew reports whether the root still has to be synced for the topic's name.
func (s *Store) topicIsNew(topic domain.TopicName) bool {
	s.mu.Lock()
	_, isNew := s.newTopics[topic]
	s.mu.Unlock()
	return isNew
}

// topicNameIsDurable is called once the root has been synced after the topic was created.
func (s *Store) topicNameIsDurable(topic domain.TopicName) {
	s.mu.Lock()
	delete(s.newTopics, topic)
	s.mu.Unlock()
}

// dirEntryIsDurable is called once the topic directory has been synced, which is what makes
// ref's name durable. Only ref is cleared: a block created while that sync was running may
// not be covered by it, and is left for its own sync to deal with.
func (s *Store) dirEntryIsDurable(ref driven.BlockRef) {
	s.mu.Lock()
	delete(s.newBlocks, ref)
	s.mu.Unlock()
}

// openForAppend opens a block for appending, creating the block and its topic if needed.
func (s *Store) openForAppend(ref driven.BlockRef) (File, error) {
	path := s.blockPath(ref)
	file, err := s.fs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, blockPerm)
	if err == nil || !os.IsNotExist(err) {
		return file, err
	}
	if mkErr := s.fs.MkdirAll(s.topicPath(ref.Topic), topicPerm); mkErr != nil {
		return nil, err
	}
	s.markNewTopic(ref.Topic)
	return s.fs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, blockPerm)
}

func (s *Store) Open(ref driven.BlockRef, byteOffset int64) (io.ReadCloser, error) {
	file, err := s.fs.OpenFile(s.blockPath(ref), os.O_RDONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, driven.ErrBlockNotFound
		}
		return nil, errore.Wrap(err)
	}
	if byteOffset > 0 {
		// a byte offset past the end reads as an empty block, which some filesystems
		// report as a torn read instead of a clean end
		info, err := file.Stat()
		if err != nil {
			closeQuietly(file)
			return nil, errore.Wrap(err)
		}
		if byteOffset > info.Size() {
			byteOffset = info.Size()
		}
		if _, err = file.Seek(byteOffset, io.SeekStart); err != nil {
			closeQuietly(file)
			return nil, errore.Wrap(err)
		}
	}
	return file, nil
}

func (s *Store) Truncate(ref driven.BlockRef, size int64) error {
	file, err := s.fs.OpenFile(s.blockPath(ref), os.O_WRONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return driven.ErrBlockNotFound
		}
		return errore.Wrap(err)
	}
	info, err := file.Stat()
	if err != nil {
		closeQuietly(file)
		return errore.Wrap(err)
	}
	if size < 0 || size > info.Size() {
		closeQuietly(file)
		return errore.WrapWithContextF(driven.ErrInvalidSize, "block %s holds %d bytes, cannot truncate to %d", ref, info.Size(), size)
	}
	if err = file.Truncate(size); err != nil {
		closeQuietly(file)
		return errore.Wrap(err)
	}
	if err = file.Close(); err != nil {
		return errore.Wrap(err)
	}
	return nil
}

func (s *Store) Remove(ref driven.BlockRef) error {
	exists, err := s.exists(s.blockPath(ref))
	if err != nil {
		return errore.Wrap(err)
	}
	if !exists {
		return driven.ErrBlockNotFound
	}
	if err = s.fs.Remove(s.blockPath(ref)); err != nil {
		return errore.Wrap(err)
	}
	return nil
}

// Sync makes the block durable: the file itself, the directory holding it when the file is
// new, and the root when the topic is, since an fsync of a file says nothing about the names
// it was reached by. A synced block in a topic whose own name is not durable is a block a
// power cut can take with the whole topic.
//
// The directory is synced only then. Appending to a block that is already named on durable
// media adds no directory entry, so syncing the directory again would buy nothing and cost
// an fsync — the same fsync a log pays on every write it acknowledges, which is the one it
// can least afford to pay twice.
func (s *Store) Sync(ref driven.BlockRef) error {
	file, err := s.fs.OpenFile(s.blockPath(ref), os.O_WRONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return driven.ErrBlockNotFound
		}
		return errore.Wrap(err)
	}
	if err = file.Sync(); err != nil {
		closeQuietly(file)
		return errore.Wrap(err)
	}
	if err = file.Close(); err != nil {
		return errore.Wrap(err)
	}
	if s.blockIsNew(ref) {
		// a failed directory sync is the caller's error as much as the block's own: the bytes
		// are on the media, but a block whose name is not is a block a power cut takes, and
		// acknowledging a write into it would be acknowledging nothing. It leaves the block
		// marked, so a later sync of it syncs the directory again.
		if err = s.syncDir(s.topicPath(ref.Topic)); err != nil {
			return errore.Wrap(err)
		}
		s.dirEntryIsDurable(ref)
	}
	if s.topicIsNew(ref.Topic) {
		// the same rule one level up, for the topic's name in the root
		if err = s.syncDir(s.rootPath); err != nil {
			return errore.Wrap(err)
		}
		s.topicNameIsDurable(ref.Topic)
	}
	return nil
}

func (s *Store) syncDir(path string) error {
	dir, err := s.fs.OpenFile(path, os.O_RDONLY, topicPerm)
	if err != nil {
		return err
	}
	if err = dir.Sync(); err != nil {
		closeQuietly(dir)
		return err
	}
	return dir.Close()
}

func closeQuietly(file File) {
	if file != nil {
		_ = file.Close()
	}
}

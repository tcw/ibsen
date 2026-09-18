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
}

var (
	_ driven.BlockStore = &Store{}
	_ driven.Syncable   = &Store{}
)

// New returns a store that keeps its topics under rootPath, which must exist, on the given
// filesystem. Pass OS{} unless you are injecting faults.
func New(fs FS, rootPath string) *Store {
	return &Store{fs: fs, rootPath: rootPath}
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
	return driven.Block{Block: ref.Block, Size: size + int64(n)}, nil
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

// Sync flushes a block to durable media, and then the topic directory so a block created by
// this append is found again after a crash. The directory flush is best effort: not every
// filesystem can do it, and a lost directory entry looks to recovery like a block that was
// never written.
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
	s.syncTopicDir(ref.Topic)
	return nil
}

func (s *Store) syncTopicDir(topic domain.TopicName) {
	dir, err := s.fs.OpenFile(s.topicPath(topic), os.O_RDONLY, topicPerm)
	if err != nil {
		return
	}
	_ = dir.Sync()
	_ = dir.Close()
}

func closeQuietly(file File) {
	if file != nil {
		_ = file.Close()
	}
}

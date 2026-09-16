// Package aferostore is the filesystem adapter behind the common.BlockStore port: a thin
// wrapper over afero that keeps every topic in a directory of numbered block files.
//
// Layout, unchanged from the original file access, is <root>/<topic>/%020d.log and .idx.
package aferostore

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	"github.com/tcw/ibsen/errore"
)

// Sep separates the elements of a path in the underlying filesystem.
const Sep = string(os.PathSeparator)

const (
	topicPerm = os.FileMode(0744)
	blockPerm = os.FileMode(0600)
)

// Store keeps blocks as files under a root directory.
type Store struct {
	afs      *afero.Afero
	rootPath string
}

var (
	_ common.BlockStore = &Store{}
	_ common.Syncable   = &Store{}
)

// New returns a store that keeps its topics under rootPath, which must exist.
func New(afs *afero.Afero, rootPath string) *Store {
	return &Store{afs: afs, rootPath: rootPath}
}

// MemAfs returns an in-memory filesystem, for tests and for the server's in-memory mode.
func MemAfs() *afero.Afero {
	return &afero.Afero{Fs: afero.NewMemMapFs()}
}

// NewMem returns a store on a fresh in-memory filesystem, together with that filesystem so
// a test can reach the bytes behind the port.
func NewMem(rootPath string) (*Store, *afero.Afero) {
	afs := MemAfs()
	if err := afs.MkdirAll(rootPath, topicPerm); err != nil {
		panic(err)
	}
	return New(afs, rootPath), afs
}

// Afs exposes the filesystem the store writes to, for wiring that also needs it.
func (s *Store) Afs() *afero.Afero {
	return s.afs
}

// RootPath is the directory the store keeps its topics under.
func (s *Store) RootPath() string {
	return s.rootPath
}

func (s *Store) topicPath(topic common.TopicName) string {
	return s.rootPath + Sep + string(topic)
}

func (s *Store) blockPath(ref common.BlockRef) string {
	return s.topicPath(ref.Topic) + Sep + blockFileName(ref)
}

func blockFileName(ref common.BlockRef) string {
	name := strconv.FormatUint(ref.Block, 10)
	return strings.Repeat("0", 20-len(name)) + name + extension(ref.Kind)
}

func extension(kind common.BlockKind) string {
	if kind == common.Index {
		return ".idx"
	}
	return ".log"
}

func (s *Store) Topics() ([]common.TopicName, error) {
	infos, err := s.afs.ReadDir(s.rootPath)
	if err != nil {
		return nil, errore.Wrap(err)
	}
	var topics []common.TopicName
	for _, info := range infos {
		// topics are directories; hidden entries and stray files are not topics
		if info.IsDir() && !strings.HasPrefix(info.Name(), ".") {
			topics = append(topics, common.TopicName(info.Name()))
		}
	}
	return topics, nil
}

func (s *Store) CreateTopic(topic common.TopicName) (bool, error) {
	path := s.topicPath(topic)
	exists, err := s.afs.DirExists(path)
	if err != nil {
		return false, errore.Wrap(err)
	}
	if exists {
		return false, nil
	}
	if err = s.afs.Mkdir(path, topicPerm); err != nil {
		// another caller may have created it after the check
		if exists, _ := s.afs.DirExists(path); exists {
			return false, nil
		}
		return false, errore.Wrap(err)
	}
	return true, nil
}

func (s *Store) List(topic common.TopicName, kind common.BlockKind) ([]common.Block, error) {
	infos, err := s.afs.ReadDir(s.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errore.Wrap(err)
	}
	want := extension(kind)
	var blocks []common.Block
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
		blocks = append(blocks, common.Block{Block: block, Size: info.Size()})
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Block < blocks[j].Block })
	return blocks, nil
}

// StrayFiles lists the names in a topic directory that are not block files, so wiring can
// report them. The port itself ignores them.
func (s *Store) StrayFiles(topic common.TopicName) ([]string, error) {
	infos, err := s.afs.ReadDir(s.topicPath(topic))
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

func (s *Store) Append(ref common.BlockRef, data []byte) (common.Block, error) {
	file, err := s.openForAppend(ref)
	if err != nil {
		return common.Block{}, errore.Wrap(err)
	}
	info, err := file.Stat()
	if err != nil {
		closeQuietly(file)
		return common.Block{}, errore.Wrap(err)
	}
	size := info.Size()
	n, err := file.Write(data)
	if err != nil {
		// leave the block as it was, so the next append does not follow a partial one
		if truncErr := file.Truncate(size); truncErr != nil {
			closeQuietly(file)
			return common.Block{}, common.DirtyBlock(errore.WrapError(truncErr, err))
		}
		closeQuietly(file)
		return common.Block{}, errore.Wrap(err)
	}
	if err = file.Close(); err != nil {
		return common.Block{}, errore.Wrap(err)
	}
	return common.Block{Block: ref.Block, Size: size + int64(n)}, nil
}

// openForAppend opens a block for appending, creating the block and its topic if needed.
func (s *Store) openForAppend(ref common.BlockRef) (afero.File, error) {
	path := s.blockPath(ref)
	file, err := s.afs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, blockPerm)
	if err == nil || !os.IsNotExist(err) {
		return file, err
	}
	if mkErr := s.afs.MkdirAll(s.topicPath(ref.Topic), topicPerm); mkErr != nil {
		return nil, err
	}
	return s.afs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, blockPerm)
}

func (s *Store) Open(ref common.BlockRef, byteOffset int64) (io.ReadCloser, error) {
	file, err := s.afs.OpenFile(s.blockPath(ref), os.O_RDONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, common.ErrBlockNotFound
		}
		return nil, errore.Wrap(err)
	}
	if byteOffset > 0 {
		if _, err = file.Seek(byteOffset, io.SeekStart); err != nil {
			closeQuietly(file)
			return nil, errore.Wrap(err)
		}
	}
	return file, nil
}

func (s *Store) Truncate(ref common.BlockRef, size int64) error {
	file, err := s.afs.OpenFile(s.blockPath(ref), os.O_WRONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return common.ErrBlockNotFound
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
		return errore.WrapWithContextF(common.ErrInvalidSize, "block %s holds %d bytes, cannot truncate to %d", ref, info.Size(), size)
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

func (s *Store) Remove(ref common.BlockRef) error {
	exists, err := s.afs.Exists(s.blockPath(ref))
	if err != nil {
		return errore.Wrap(err)
	}
	if !exists {
		return common.ErrBlockNotFound
	}
	if err = s.afs.Remove(s.blockPath(ref)); err != nil {
		return errore.Wrap(err)
	}
	return nil
}

// Sync flushes a block to durable media. It syncs the block's bytes, not the directory
// entry: a block that was created but never synced can still be lost by a crash, which
// recovery handles as a missing tail.
func (s *Store) Sync(ref common.BlockRef) error {
	file, err := s.afs.OpenFile(s.blockPath(ref), os.O_WRONLY, blockPerm)
	if err != nil {
		if os.IsNotExist(err) {
			return common.ErrBlockNotFound
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
	return nil
}

func closeQuietly(file afero.File) {
	if file != nil {
		_ = file.Close()
	}
}

package faultfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
)

// ErrSyncFailed is what an injected fsync failure gives, standing in for EIO.
var ErrSyncFailed = errors.New("input/output error (injected fsync failure)")

// PageCache is a filesystem that knows what has reached the media and what has only reached
// the page cache, which is the difference a power cut exposes and a killed process does not.
// It is the LazyFS idea on the seam this repository already has: every call really happens on
// a real directory, so reads, opens and creates behave as the kernel makes them behave, and
// beside that the model keeps what the media holds. Crash stops the filesystem; Recover
// rewrites the directory to one state the media could be in afterwards, chosen by a Loss.
//
// What it models, and nothing more:
//
//   - A file's data reaches the media when that file is synced. Until then a crash keeps some
//     prefix of the writes and truncates made since the last sync, the last write possibly
//     torn, and nothing after.
//   - A name reaches the media when the directory holding it is synced, as POSIX has it. A file
//     or directory created or removed since its parent was last synced may or may not be there
//     after a crash, whatever was synced inside it. ext4 is kinder than this in practice, since
//     its fsync commits the journal the create is in; the model is not, because nothing
//     promises that.
//   - A failed fsync loses the data it failed to write, as Linux does since 4.13: the pages it
//     was asked to write are marked clean and dropped from writeback, so they stay readable from
//     the cache, a later fsync succeeds without writing them, and after a crash the range reads
//     as zeros. That is "fsyncgate", and it is why retrying a failed fsync is not a retry.
//
// Nothing here models reordering between files, a disk that lies about flushing, or a torn
// page inside a range that was synced.
type PageCache struct {
	base filestore.FS

	mu      sync.Mutex
	crashed bool
	files   map[string]*cachedFile
	// fresh holds the names created since their parent directory was last synced, and whether
	// each is a directory; removed holds what a name that was durable held when it was removed
	fresh   map[string]bool
	removed map[string][]byte
	// failSyncs is how many of the next file syncs matching failSuffix fail
	failSyncs  int
	failSuffix string
	// calls is the trace of the calls that change the media, as far as counts lets them in;
	// crashAt and failAt name one of them by its number, from 1
	calls   []Call
	counts  func(path string) bool
	crashAt int
	failAt  int
}

// Call is one call that changes the media: its kind — mkdir, create, write, truncate, sync
// or remove — and the path it was made on. A trace of them is what a crash can be aimed at.
type Call struct {
	Kind string
	Path string
}

// cachedFile is one file as the media holds it and as the cache has changed it since.
type cachedFile struct {
	disk    []byte
	pending []pendingOp
	// lost is what a failed fsync dropped: ranges the cache still returns and the media will
	// never hold, unless they are written again
	lost []byteRange
}

type pendingOp struct {
	truncate bool
	offset   int64
	data     []byte
	size     int64
}

type byteRange struct{ from, to int64 }

var _ filestore.FS = &PageCache{}

// NewPageCache wraps the real filesystem. Files are taken to be on the media as they are when
// first opened through it, so it can be pointed at a directory that already holds a log.
func NewPageCache() *PageCache {
	return &PageCache{
		base:    filestore.OS{},
		files:   map[string]*cachedFile{},
		fresh:   map[string]bool{},
		removed: map[string][]byte{},
	}
}

// FailSyncs makes the next count syncs of a file whose name ends in suffix fail, with Linux
// semantics: what they were asked to write is lost to the media, though still readable.
func (c *PageCache) FailSyncs(suffix string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failSuffix = suffix
	c.failSyncs = count
}

// CountOnly limits the trace, and so what CrashAtCall and FailAtCall count, to calls on paths
// count accepts. A call that is not counted still happens and is still modelled. It is for
// keeping the trace of a workload the same from run to run when part of it is written by a
// goroutine on its own schedule.
func (c *PageCache) CountOnly(count func(path string) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = count
}

// CrashAtCall cuts the power as call n of the trace begins, so that call fails and nothing
// after it happens; 0 never does.
func (c *PageCache) CrashAtCall(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.crashAt = n
}

// FailAtCall makes call n of the trace fail if it is a sync, as FailSyncs does for a file,
// and for a directory by leaving the names in it as undurable as they were; 0 never does.
func (c *PageCache) FailAtCall(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failAt = n
}

// Trace returns the counted calls so far, in the order they were made.
func (c *PageCache) Trace() []Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Call(nil), c.calls...)
}

// step records a call that changes the media, and reports whether it is the one to fail or
// the one the power goes out at. Called with c.mu held.
func (c *PageCache) step(kind, path string) (fail bool, crash bool) {
	if c.counts != nil && !c.counts(path) {
		return false, false
	}
	c.calls = append(c.calls, Call{Kind: kind, Path: path})
	n := len(c.calls)
	if n == c.crashAt {
		c.crashed = true
		return false, true
	}
	return n == c.failAt, false
}

// DurableContains reports whether needle is in some file under root that the media holds
// under a durable name, every directory on the way to root included. It is the question a
// promise to a client rests on: an acknowledged write, or an entry a reader was given, has to
// be there after any crash from that moment on, so it has to be durable at that moment.
func (c *PageCache) DurableContains(root string, needle []byte) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	root = filepath.Clean(root)
	durableName := func(path string) bool {
		for p := path; p != root && p != filepath.Dir(p); p = filepath.Dir(p) {
			if _, fresh := c.fresh[p]; fresh {
				return false
			}
		}
		return true
	}
	for path, content := range c.removed {
		if durableName(filepath.Dir(path)) && bytes.Contains(content, needle) {
			return true, nil
		}
	}
	found := false
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || found || entry.IsDir() || !durableName(path) {
			return err
		}
		var content []byte
		if file, tracked := c.files[path]; tracked {
			content = zeroRanges(append([]byte(nil), file.disk...), file.lost)
		} else if content, err = os.ReadFile(path); err != nil {
			return err
		}
		found = bytes.Contains(content, needle)
		return nil
	})
	return found, err
}

// Crash stops the filesystem: every call from now on fails with ErrCrashed, as for a machine
// that has lost power. Nothing on disk changes until Recover.
func (c *PageCache) Crash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.crashed = true
}

// Crashed reports whether Crash has been called since the last Recover.
func (c *PageCache) Crashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}

// Loss decides what that was not on the media survives a crash anyway.
type Loss interface {
	// keepOps is how many of a file's unsynced operations survive, in order, and how many
	// bytes of the write after them, if there is one
	keepOps(path string, pending []pendingOp) (kept int, torn int)
	// keepEntry reports whether a change to a directory entry that was not synced, a name
	// created or a name removed, survives the crash
	keepEntry(path string) bool
}

// LoseEverything keeps nothing that was not synced: every unsynced write, and every name
// created or removed since its directory was synced, is gone. It is the harshest crash the
// model allows, and the one every acknowledged write has to survive.
func LoseEverything() Loss { return loseEverything{} }

type loseEverything struct{}

func (loseEverything) keepOps(string, []pendingOp) (int, int) { return 0, 0 }
func (loseEverything) keepEntry(string) bool                  { return false }

// LoseSome keeps a random amount of what was not synced: a prefix of each file's unsynced
// operations with the next write torn at a random byte, and each undurable name with even
// odds. The same seed makes the same choices for the same calls.
func LoseSome(seed uint64) Loss {
	return &loseSome{rand: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

type loseSome struct{ rand *rand.Rand }

func (l *loseSome) keepOps(_ string, pending []pendingOp) (int, int) {
	kept := l.rand.IntN(len(pending) + 1)
	if kept == len(pending) || pending[kept].truncate || len(pending[kept].data) == 0 {
		return kept, 0
	}
	return kept, l.rand.IntN(len(pending[kept].data))
}

func (l *loseSome) keepEntry(string) bool { return l.rand.IntN(2) == 0 }

// KeepBytes keeps, of each file's unsynced writes, as many bytes as keep says, in the order
// they were written, and keeps every unsynced change to a directory. It is for a test that
// needs one particular crash rather than a random one: that one file's tail reached the media
// and another's did not, which writeback is free to do in any order.
func KeepBytes(keep func(path string, unsynced int) int) Loss {
	return keepBytes(keep)
}

type keepBytes func(path string, unsynced int) int

func (k keepBytes) keepOps(path string, pending []pendingOp) (int, int) {
	unsynced := 0
	for _, op := range pending {
		unsynced += len(op.data)
	}
	budget := k(path, unsynced)
	for i, op := range pending {
		if len(op.data) > budget {
			return i, budget
		}
		budget -= len(op.data)
	}
	return len(pending), 0
}

func (keepBytes) keepEntry(string) bool { return true }

// Recover rewrites the directory to what the media holds after the crash, as loss decides,
// and makes the filesystem usable again with that as its durable state. It must follow Crash.
func (c *PageCache) Recover(loss Loss) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.crashed {
		return errors.New("recover without a crash")
	}
	// the files first, so a name that is then lost takes its file with it
	paths := sortedKeys(c.files)
	for _, path := range paths {
		file := c.files[path]
		image := file.imageAfterCrash(path, loss)
		if err := os.WriteFile(path, image, 0600); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	for _, path := range sortedKeys(c.removed) {
		if !loss.keepEntry(path) {
			// the removal did not reach the media, so the file is back
			if err := os.MkdirAll(filepath.Dir(path), 0744); err != nil {
				return err
			}
			if err := os.WriteFile(path, c.removed[path], 0600); err != nil {
				return err
			}
		}
	}
	for _, path := range sortedKeys(c.fresh) {
		if !loss.keepEntry(path) {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		}
	}
	// what is there now is what the media holds
	c.files = map[string]*cachedFile{}
	c.fresh = map[string]bool{}
	c.removed = map[string][]byte{}
	c.failSyncs = 0
	c.crashAt = 0
	c.failAt = 0
	c.crashed = false
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// imageAfterCrash is what the media holds for the file once the crash is over: what was
// synced, then what loss lets through of what was not, with anything a failed fsync dropped
// reading as zeros.
func (f *cachedFile) imageAfterCrash(path string, loss Loss) []byte {
	image := append([]byte(nil), f.disk...)
	if len(f.pending) > 0 {
		kept, torn := loss.keepOps(path, f.pending)
		image = applyOps(image, f.pending[:kept])
		if torn > 0 {
			next := f.pending[kept]
			image = applyOps(image, []pendingOp{{offset: next.offset, data: next.data[:torn]}})
		}
	}
	return zeroRanges(image, f.lost)
}

func applyOps(image []byte, ops []pendingOp) []byte {
	for _, op := range ops {
		if op.truncate {
			if op.size < int64(len(image)) {
				image = image[:op.size]
			} else {
				image = append(image, make([]byte, op.size-int64(len(image)))...)
			}
			continue
		}
		end := op.offset + int64(len(op.data))
		if end > int64(len(image)) {
			image = append(image, make([]byte, end-int64(len(image)))...)
		}
		copy(image[op.offset:], op.data)
	}
	return image
}

func zeroRanges(image []byte, ranges []byteRange) []byte {
	for _, r := range ranges {
		for i := r.from; i < r.to && i < int64(len(image)); i++ {
			image[i] = 0
		}
	}
	return image
}

// without removes what a new write covers from the lost ranges: those bytes are dirty again,
// and the next fsync will write them.
func without(ranges []byteRange, cut byteRange) []byteRange {
	var kept []byteRange
	for _, r := range ranges {
		if r.to <= cut.from || r.from >= cut.to {
			kept = append(kept, r)
			continue
		}
		if r.from < cut.from {
			kept = append(kept, byteRange{r.from, cut.from})
		}
		if r.to > cut.to {
			kept = append(kept, byteRange{cut.to, r.to})
		}
	}
	return kept
}

// track returns the model of a file, taking what is on disk now as what the media holds when
// the file has not been seen before. Called with c.mu held.
func (c *PageCache) track(path string) *cachedFile {
	if file, ok := c.files[path]; ok {
		return file
	}
	content, err := os.ReadFile(path)
	if err != nil {
		content = nil
	}
	file := &cachedFile{disk: content}
	c.files[path] = file
	return file
}

// created records a name that is not durable until its parent directory is synced.
func (c *PageCache) created(path string, isDir bool) {
	if _, wasRemoved := c.removed[path]; wasRemoved {
		// the old file's name is still what the media holds; the new one replaces it only once
		// the directory is synced, and a crash before that leaves either
		return
	}
	c.fresh[path] = isDir
}

func (c *PageCache) Stat(name string) (os.FileInfo, error) {
	if c.Crashed() {
		return nil, ErrCrashed
	}
	return c.base.Stat(name)
}

func (c *PageCache) ReadDir(name string) ([]os.FileInfo, error) {
	if c.Crashed() {
		return nil, ErrCrashed
	}
	return c.base.ReadDir(name)
}

func (c *PageCache) Mkdir(name string, perm os.FileMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return ErrCrashed
	}
	if _, crash := c.step("mkdir", filepath.Clean(name)); crash {
		return ErrCrashed
	}
	if err := c.base.Mkdir(name, perm); err != nil {
		return err
	}
	c.created(filepath.Clean(name), true)
	return nil
}

func (c *PageCache) MkdirAll(name string, perm os.FileMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return ErrCrashed
	}
	var missing []string
	for dir := filepath.Clean(name); ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(dir); err == nil {
			break
		}
		missing = append(missing, dir)
		if filepath.Dir(dir) == dir {
			break
		}
	}
	if len(missing) > 0 {
		if _, crash := c.step("mkdir", filepath.Clean(name)); crash {
			return ErrCrashed
		}
	}
	if err := c.base.MkdirAll(name, perm); err != nil {
		return err
	}
	for _, dir := range missing {
		c.created(dir, true)
	}
	return nil
}

func (c *PageCache) Remove(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return ErrCrashed
	}
	path := filepath.Clean(name)
	if _, crash := c.step("remove", path); crash {
		return ErrCrashed
	}
	file := c.track(path)
	if err := c.base.Remove(name); err != nil {
		return err
	}
	if _, isFresh := c.fresh[path]; isFresh {
		// a name the media never held goes without a trace
		delete(c.fresh, path)
	} else {
		c.removed[path] = zeroRanges(append([]byte(nil), file.disk...), file.lost)
	}
	delete(c.files, path)
	return nil
}

func (c *PageCache) OpenFile(name string, flag int, perm os.FileMode) (filestore.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return nil, ErrCrashed
	}
	path := filepath.Clean(name)
	info, statErr := os.Stat(path)
	existed := statErr == nil
	isDir := existed && info.IsDir()
	if existed && !isDir {
		// what is on disk before the open is what the media holds, if it is news to the model
		c.track(path)
	}
	if !existed && flag&os.O_CREATE != 0 {
		if _, crash := c.step("create", path); crash {
			return nil, ErrCrashed
		}
	}
	file, err := c.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if !existed {
		c.created(path, false)
		c.files[path] = &cachedFile{}
	}
	if existed && !isDir && flag&os.O_TRUNC != 0 {
		c.files[path].pending = append(c.files[path].pending, pendingOp{truncate: true, size: 0})
	}
	return &pageCacheHandle{File: file, path: path, isDir: isDir, appends: flag&os.O_APPEND != 0, cache: c}, nil
}

type pageCacheHandle struct {
	filestore.File
	path    string
	isDir   bool
	appends bool
	cache   *PageCache
}

func (h *pageCacheHandle) Read(p []byte) (int, error) {
	if h.cache.Crashed() {
		return 0, ErrCrashed
	}
	return h.File.Read(p)
}

func (h *pageCacheHandle) Write(p []byte) (int, error) {
	c := h.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return 0, ErrCrashed
	}
	if _, crash := c.step("write", h.path); crash {
		return 0, ErrCrashed
	}
	var offset int64
	var err error
	if h.appends {
		var info os.FileInfo
		if info, err = h.File.Stat(); err == nil {
			offset = info.Size()
		}
	} else {
		offset, err = h.File.Seek(0, io.SeekCurrent)
	}
	if err != nil {
		return 0, err
	}
	n, err := h.File.Write(p)
	if n > 0 {
		file := c.track(h.path)
		file.pending = append(file.pending, pendingOp{offset: offset, data: append([]byte(nil), p[:n]...)})
		file.lost = without(file.lost, byteRange{offset, offset + int64(n)})
	}
	return n, err
}

func (h *pageCacheHandle) Truncate(size int64) error {
	c := h.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return ErrCrashed
	}
	if _, crash := c.step("truncate", h.path); crash {
		return ErrCrashed
	}
	if err := h.File.Truncate(size); err != nil {
		return err
	}
	file := c.track(h.path)
	file.pending = append(file.pending, pendingOp{truncate: true, size: size})
	return nil
}

// Sync of a directory makes the names in it durable. Sync of a file makes what was written to
// it durable, or, when an injected failure hits it, loses that for good.
func (h *pageCacheHandle) Sync() error {
	c := h.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.crashed {
		return ErrCrashed
	}
	fail, crash := c.step("sync", h.path)
	if crash {
		return ErrCrashed
	}
	if h.isDir {
		if fail {
			// the names in it are exactly as undurable as they were
			return ErrSyncFailed
		}
		for path := range c.fresh {
			if filepath.Dir(path) == h.path {
				delete(c.fresh, path)
			}
		}
		for path := range c.removed {
			if filepath.Dir(path) == h.path {
				delete(c.removed, path)
			}
		}
		return nil
	}
	file := c.track(h.path)
	if fail || c.failSyncs > 0 && strings.HasSuffix(h.path, c.failSuffix) {
		if !fail {
			c.failSyncs--
		}
		// the pages this fsync was asked to write are marked clean without being written:
		// they stay readable, and no later fsync will write them
		var truncates []pendingOp
		for _, op := range file.pending {
			if op.truncate {
				truncates = append(truncates, op)
				continue
			}
			file.lost = append(file.lost, byteRange{op.offset, op.offset + int64(len(op.data))})
		}
		file.pending = truncates
		return ErrSyncFailed
	}
	file.disk = zeroRanges(applyOps(file.disk, file.pending), file.lost)
	file.pending = nil
	return nil
}

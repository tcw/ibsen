package flashstore

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/tcw/ibsen/access/common"
)

// A page holds a header, a fill bitmap and a data area. The header names the block the page
// belongs to and its place in that block, so the page table can be rebuilt by reading the
// region. The bitmap has one bit per data byte, cleared as the byte is written: a length
// field could not be updated without an erase, but clearing one more bit always can be.
const (
	magic0     = 0x49 // 'I'
	magic1     = 0x42 // 'B'
	offMagic   = 0
	offKind    = 2
	offNameLen = 3
	offBlock   = 4
	offSeq     = 12
	offName    = 16
	maxNameLen = 32
	headerSize = offName + maxNameLen
)

// MaxTopicNameLength is the longest topic name a page header can hold. It is shorter than
// the log's own limit, which is what an adapter with a fixed page layout costs.
const MaxTopicNameLength = maxNameLen

// Store is a BlockStore over a flash region. The page table lives in RAM and is rebuilt by
// reading every page when the store opens.
type Store struct {
	mu         sync.Mutex
	dev        Device
	dataSize   int
	bitmapSize int
	dataOffset int
	buf        []byte
	free       []int
	topics     map[common.TopicName]map[blockKey]*blockMeta
}

var _ common.BlockStore = &Store{}

type blockKey struct {
	kind  common.BlockKind
	block uint64
}

type blockMeta struct {
	pages []pageState
	size  int64
}

// pageState is a page of a block and how many of its data bytes are written.
type pageState struct {
	page int
	used int
}

// New opens a store on a region, reading every page to rebuild the page table.
func New(dev Device) (*Store, error) {
	pageSize := dev.PageSize()
	// every data byte costs one bitmap bit, so nine bits of page per byte of data
	dataSize := (pageSize - headerSize) * 8 / 9
	dataSize -= dataSize % 8
	if dataSize < 8 {
		return nil, fmt.Errorf("%w: a page of %d bytes is too small for a header of %d", ErrOutOfRange, pageSize, headerSize)
	}
	s := &Store{
		dev:        dev,
		dataSize:   dataSize,
		bitmapSize: dataSize / 8,
		dataOffset: headerSize + dataSize/8,
		buf:        make([]byte, pageSize),
		topics:     make(map[common.TopicName]map[blockKey]*blockMeta),
	}
	if err := s.scan(); err != nil {
		return nil, err
	}
	return s, nil
}

// DataPerPage is the number of log bytes a page holds, the rest being the header and the
// fill bitmap.
func (s *Store) DataPerPage() int {
	return s.dataSize
}

// FreePages is how many pages the region still has for new data.
func (s *Store) FreePages() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.free)
}

type pageInfo struct {
	topic common.TopicName
	key   blockKey
	seq   uint32
	page  int
	used  int
}

func (s *Store) scan() error {
	var infos []pageInfo
	for page := 0; page < s.dev.Pages(); page++ {
		if err := s.dev.ReadPage(page, s.buf); err != nil {
			return err
		}
		if s.buf[offMagic] == 0xff && s.buf[offMagic+1] == 0xff {
			s.free = append(s.free, page)
			continue
		}
		if s.buf[offMagic] != magic0 || s.buf[offMagic+1] != magic1 {
			// programmed, but not by us: leave it alone rather than erase it
			continue
		}
		nameLen := int(s.buf[offNameLen])
		if nameLen == 0 || nameLen > maxNameLen {
			continue
		}
		infos = append(infos, pageInfo{
			topic: common.TopicName(s.buf[offName : offName+nameLen]),
			key: blockKey{
				kind:  common.BlockKind(s.buf[offKind]),
				block: readUint64(s.buf[offBlock:]),
			},
			seq:  readUint32(s.buf[offSeq:]),
			page: page,
			used: usedBytes(s.buf[headerSize:headerSize+s.bitmapSize], s.dataSize),
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].topic != infos[j].topic {
			return infos[i].topic < infos[j].topic
		}
		if infos[i].key != infos[j].key {
			if infos[i].key.kind != infos[j].key.kind {
				return infos[i].key.kind < infos[j].key.kind
			}
			return infos[i].key.block < infos[j].key.block
		}
		return infos[i].seq < infos[j].seq
	})
	for _, info := range infos {
		blocks, exists := s.topics[info.topic]
		if !exists {
			blocks = make(map[blockKey]*blockMeta)
			s.topics[info.topic] = blocks
		}
		meta := blocks[info.key]
		if meta == nil {
			meta = &blockMeta{}
			blocks[info.key] = meta
		}
		if int(info.seq) != len(meta.pages) {
			// a page of this block is missing, so the block ends where the chain breaks
			continue
		}
		meta.pages = append(meta.pages, pageState{page: info.page, used: info.used})
		meta.size += int64(info.used)
	}
	return nil
}

func (s *Store) Topics() ([]common.TopicName, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var topics []common.TopicName
	for topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i] < topics[j] })
	return topics, nil
}

// CreateTopic remembers a topic that holds no blocks yet. A region has no directory to put
// it in, so an empty topic lives in RAM until something is written to it.
func (s *Store) CreateTopic(topic common.TopicName) (bool, error) {
	if err := checkName(topic); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.topics[topic]; exists {
		return false, nil
	}
	s.topics[topic] = make(map[blockKey]*blockMeta)
	return true, nil
}

func (s *Store) List(topic common.TopicName, kind common.BlockKind) ([]common.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var blocks []common.Block
	for key, meta := range s.topics[topic] {
		if key.kind != kind {
			continue
		}
		blocks = append(blocks, common.Block{Block: key.block, Size: meta.size})
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Block < blocks[j].Block })
	return blocks, nil
}

// chunk is a run of bytes going into one page of a block.
type chunk struct {
	pageIdx int
	page    int
	offset  int
	data    []byte
}

func (s *Store) Append(ref common.BlockRef, data []byte) (common.Block, error) {
	if err := checkName(ref.Topic); err != nil {
		return common.Block{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks, exists := s.topics[ref.Topic]
	if !exists {
		blocks = make(map[blockKey]*blockMeta)
		s.topics[ref.Topic] = blocks
	}
	key := keyOf(ref)
	meta := blocks[key]
	if meta == nil {
		meta = &blockMeta{}
	}

	newPages, chunks := s.plan(meta, data)
	if newPages > len(s.free) {
		return common.Block{}, fmt.Errorf("%w: %d pages needed, %d left", ErrNoSpace, newPages, len(s.free))
	}
	claimed, err := s.claim(ref, len(meta.pages), newPages)
	if err != nil {
		return common.Block{}, err
	}
	for i := range chunks {
		if chunks[i].page < 0 {
			chunks[i].page = claimed[chunks[i].pageIdx-len(meta.pages)]
		}
	}

	// the data goes down first and the bitmap after it, so a program that fails leaves
	// bytes nothing points at rather than a half visible append
	for _, c := range chunks {
		if len(c.data) == 0 {
			continue
		}
		if err := s.dev.ProgramPage(c.page, s.dataOffset+c.offset, c.data); err != nil {
			s.release(claimed)
			return common.Block{}, err
		}
	}
	for i, c := range chunks {
		if len(c.data) == 0 {
			continue
		}
		if err := s.markUsed(c.page, c.offset, len(c.data)); err != nil {
			s.release(claimed)
			if i > 0 || c.pageIdx < len(meta.pages) {
				// part of the append is already visible and cannot be taken back
				s.refresh(meta)
				blocks[key] = meta
				return common.Block{}, common.DirtyBlock(err)
			}
			return common.Block{}, err
		}
	}

	for _, page := range claimed {
		meta.pages = append(meta.pages, pageState{page: page})
	}
	for _, c := range chunks {
		meta.pages[c.pageIdx].used = c.offset + len(c.data)
	}
	meta.size += int64(len(data))
	blocks[key] = meta
	return common.Block{Block: ref.Block, Size: meta.size}, nil
}

// plan works out how many new pages an append needs and which bytes go where. A page that
// does not exist yet is left at -1 until it is claimed.
func (s *Store) plan(meta *blockMeta, data []byte) (int, []chunk) {
	var chunks []chunk
	pos := 0
	if len(meta.pages) > 0 {
		last := len(meta.pages) - 1
		room := s.dataSize - meta.pages[last].used
		if room > len(data) {
			room = len(data)
		}
		if room > 0 {
			chunks = append(chunks, chunk{pageIdx: last, page: meta.pages[last].page, offset: meta.pages[last].used, data: data[:room]})
			pos = room
		}
	}
	newPages := 0
	for pos < len(data) || (len(meta.pages) == 0 && newPages == 0) {
		n := len(data) - pos
		if n > s.dataSize {
			n = s.dataSize
		}
		chunks = append(chunks, chunk{pageIdx: len(meta.pages) + newPages, page: -1, offset: 0, data: data[pos : pos+n]})
		pos += n
		newPages++
	}
	return newPages, chunks
}

// claim takes pages from the free list and stamps each with the block it now belongs to.
func (s *Store) claim(ref common.BlockRef, firstSeq int, count int) ([]int, error) {
	claimed := make([]int, 0, count)
	for i := 0; i < count; i++ {
		page := s.free[len(s.free)-1]
		s.free = s.free[:len(s.free)-1]
		if err := s.writeHeader(page, ref, uint32(firstSeq+i)); err != nil {
			s.free = append(s.free, page)
			s.release(claimed)
			return nil, err
		}
		claimed = append(claimed, page)
	}
	return claimed, nil
}

// release erases pages an append claimed but could not use, and puts them back.
func (s *Store) release(pages []int) {
	for _, page := range pages {
		if err := s.dev.ErasePage(page); err != nil {
			// an unerasable page is a worn out page: drop it rather than reuse it
			continue
		}
		s.free = append(s.free, page)
	}
}

// refresh rereads how much of each page of a block is written, after a failure left RAM and
// the region disagreeing.
func (s *Store) refresh(meta *blockMeta) {
	meta.size = 0
	for i, page := range meta.pages {
		if err := s.dev.ReadPage(page.page, s.buf); err != nil {
			continue
		}
		meta.pages[i].used = usedBytes(s.buf[headerSize:headerSize+s.bitmapSize], s.dataSize)
		meta.size += int64(meta.pages[i].used)
	}
}

func (s *Store) Open(ref common.BlockRef, byteOffset int64) (io.ReadCloser, error) {
	s.mu.Lock()
	meta := s.topics[ref.Topic][keyOf(ref)]
	if meta == nil {
		s.mu.Unlock()
		return nil, common.ErrBlockNotFound
	}
	pages := append([]pageState(nil), meta.pages...)
	s.mu.Unlock()
	if byteOffset < 0 {
		return nil, common.ErrInvalidSize
	}
	return newBlockReader(s.dev, s.dataOffset, pages, byteOffset), nil
}

func (s *Store) Truncate(ref common.BlockRef, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := s.topics[ref.Topic][keyOf(ref)]
	if meta == nil {
		return common.ErrBlockNotFound
	}
	if size < 0 || size > meta.size {
		return fmt.Errorf("%w: block %s holds %d bytes, cannot truncate to %d", common.ErrInvalidSize, ref, meta.size, size)
	}
	keep := int((size + int64(s.dataSize) - 1) / int64(s.dataSize))
	if keep == 0 {
		// a block always keeps one page, so it does not disappear when it is emptied
		keep = 1
	}
	for i := keep; i < len(meta.pages); i++ {
		s.release([]int{meta.pages[i].page})
	}
	meta.pages = meta.pages[:keep]
	lastUsed := int(size) - (keep-1)*s.dataSize
	if lastUsed < 0 {
		lastUsed = 0
	}
	if meta.pages[keep-1].used != lastUsed {
		// flash cannot unwrite a byte, so the page is read out, erased and written again
		if err := s.rewritePage(ref, meta, keep-1, lastUsed); err != nil {
			s.refresh(meta)
			return common.DirtyBlock(err)
		}
	}
	meta.size = size
	return nil
}

func (s *Store) rewritePage(ref common.BlockRef, meta *blockMeta, idx int, used int) error {
	page := meta.pages[idx].page
	if err := s.dev.ReadPage(page, s.buf); err != nil {
		return err
	}
	kept := append([]byte(nil), s.buf[s.dataOffset:s.dataOffset+used]...)
	if err := s.dev.ErasePage(page); err != nil {
		return err
	}
	if err := s.writeHeader(page, ref, uint32(idx)); err != nil {
		return err
	}
	if used > 0 {
		if err := s.dev.ProgramPage(page, s.dataOffset, kept); err != nil {
			return err
		}
		if err := s.markUsed(page, 0, used); err != nil {
			return err
		}
	}
	meta.pages[idx].used = used
	return nil
}

func (s *Store) Remove(ref common.BlockRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks, exists := s.topics[ref.Topic]
	if !exists {
		return common.ErrBlockNotFound
	}
	key := keyOf(ref)
	meta := blocks[key]
	if meta == nil {
		return common.ErrBlockNotFound
	}
	for _, page := range meta.pages {
		s.release([]int{page.page})
	}
	delete(blocks, key)
	return nil
}

func (s *Store) writeHeader(page int, ref common.BlockRef, seq uint32) error {
	header := make([]byte, headerSize)
	for i := range header {
		header[i] = 0xff
	}
	header[offMagic] = magic0
	header[offMagic+1] = magic1
	header[offKind] = byte(ref.Kind)
	header[offNameLen] = byte(len(ref.Topic))
	putUint64(header[offBlock:], ref.Block)
	putUint32(header[offSeq:], seq)
	copy(header[offName:], ref.Topic)
	return s.dev.ProgramPage(page, 0, header)
}

// markUsed clears the bitmap bits of the data bytes an append just wrote.
func (s *Store) markUsed(page int, from int, count int) error {
	if err := s.dev.ReadPage(page, s.buf); err != nil {
		return err
	}
	first := from / 8
	last := (from + count - 1) / 8
	bitmap := append([]byte(nil), s.buf[headerSize+first:headerSize+last+1]...)
	for i := from; i < from+count; i++ {
		bitmap[i/8-first] &^= 1 << uint(i%8)
	}
	return s.dev.ProgramPage(page, headerSize+first, bitmap)
}

// usedBytes counts the data bytes a page holds: the run of cleared bits the bitmap starts with.
func usedBytes(bitmap []byte, dataSize int) int {
	used := 0
	for i := 0; i < dataSize; i++ {
		if bitmap[i/8]&(1<<uint(i%8)) != 0 {
			break
		}
		used++
	}
	return used
}

func keyOf(ref common.BlockRef) blockKey {
	return blockKey{kind: ref.Kind, block: ref.Block}
}

func checkName(topic common.TopicName) error {
	if len(topic) == 0 || len(topic) > maxNameLen {
		return fmt.Errorf("%w: a page header holds a name of 1 to %d bytes, %q is %d",
			common.ErrInvalidTopicName, maxNameLen, topic, len(topic))
	}
	return nil
}

func readUint64(b []byte) uint64 {
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func putUint64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * uint(i)))
	}
}

func readUint32(b []byte) uint32 {
	var v uint32
	for i := 3; i >= 0; i-- {
		v = v<<8 | uint32(b[i])
	}
	return v
}

func putUint32(b []byte, v uint32) {
	for i := 0; i < 4; i++ {
		b[i] = byte(v >> (8 * uint(i)))
	}
}

// blockReader walks the pages of a block, reading one page at a time so an embedded build
// never has to hold a whole block in memory.
type blockReader struct {
	dev        Device
	dataOffset int
	pages      []pageState
	buf        []byte
	loaded     int
	idx        int
	off        int
}

func newBlockReader(dev Device, dataOffset int, pages []pageState, byteOffset int64) *blockReader {
	r := &blockReader{dev: dev, dataOffset: dataOffset, pages: pages, buf: make([]byte, dev.PageSize()), loaded: -1}
	for r.idx < len(r.pages) && byteOffset >= int64(r.pages[r.idx].used) {
		byteOffset -= int64(r.pages[r.idx].used)
		r.idx++
	}
	if r.idx < len(r.pages) {
		r.off = int(byteOffset)
	}
	return r
}

func (r *blockReader) Read(p []byte) (int, error) {
	read := 0
	for read < len(p) {
		if r.idx >= len(r.pages) {
			break
		}
		available := r.pages[r.idx].used - r.off
		if available <= 0 {
			r.idx++
			r.off = 0
			continue
		}
		if r.loaded != r.idx {
			if err := r.dev.ReadPage(r.pages[r.idx].page, r.buf); err != nil {
				return read, err
			}
			r.loaded = r.idx
		}
		from := r.dataOffset + r.off
		n := copy(p[read:], r.buf[from:from+available])
		read += n
		r.off += n
	}
	if read == 0 {
		return 0, io.EOF
	}
	return read, nil
}

func (r *blockReader) Close() error {
	return nil
}

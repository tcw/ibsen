package topic

// Compressing a frame costs CPU whether or not the result is kept, and a frame that does not
// shrink is stored plain — so on data that does not compress, every attempt is paid for and
// thrown away. Measured (ARCHITECTURE.md §13.4): a one-entry 64-byte frame costs 1.1 µs through
// zstd against 0.06 µs stored plain, and random or already-compressed payloads are never kept
// at any size, at five to fifteen times the cost of not trying. Two things avoid the attempt
// where it is unlikely to pay. Neither changes the format: a frame says which codec wrote it,
// so a reader cannot tell a frame that was not tried from one that did not pay.
const (
	// AdaptiveMisses is how many frames in a row may be stored plain because compression did
	// not pay before a topic stops trying.
	AdaptiveMisses = 16
	// AdaptiveProbeEvery is how often a topic that stopped trying tries again, in frames, in
	// case its data changed.
	AdaptiveProbeEvery = 64
)

// compressSkip decides, frame by frame, whether to offer a frame to the codec at all. Guarded
// by Topic.mu, like everything a write touches.
type compressSkip struct {
	// minBytes is the smallest plain payload worth trying; zero tries every frame
	minBytes int
	// adaptive stops trying after AdaptiveMisses frames in a row did not pay
	adaptive bool
	misses   int
	skipping bool
	frames   int
}

// minBytesFor is the threshold for a frame. A dictionary pays on far smaller frames than the
// codec alone — from ~48 bytes against ~128 on JSON events — so with one the threshold is three
// eighths of what it is without.
func (c *compressSkip) minBytesFor(withDictionary bool) int {
	if withDictionary {
		return c.minBytes * 3 / 8
	}
	return c.minBytes
}

// try reports whether a frame of size plain bytes should be offered to the codec.
func (c *compressSkip) try(size int, withDictionary bool) bool {
	if size < c.minBytesFor(withDictionary) {
		return false
	}
	if !c.adaptive || !c.skipping {
		return true
	}
	c.frames++
	if c.frames < AdaptiveProbeEvery {
		return false
	}
	c.frames = 0
	return true
}

// tried records whether an attempt paid: one that did ends any skipping, and AdaptiveMisses
// in a row that did not start it.
func (c *compressSkip) tried(paid bool) {
	if paid {
		c.misses, c.skipping, c.frames = 0, false, 0
		return
	}
	c.misses++
	if c.adaptive && c.misses >= AdaptiveMisses {
		c.skipping = true
	}
}

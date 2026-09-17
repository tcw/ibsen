package index

import (
	"fmt"
	"testing"

	"github.com/tcw/ibsen/core/domain"
)

// linearNearest is the scan back from the end that the binary search replaced, kept as the
// reference the search has to agree with.
func linearNearest(pairs []domain.OffsetFilePtr, offset domain.Offset) domain.OffsetFilePtr {
	for i := len(pairs) - 1; i >= 0; i-- {
		if offset >= pairs[i].Offset {
			return pairs[i]
		}
	}
	return domain.OffsetFilePtr{}
}

// pairs builds an index whose offsets start at first and step by every.
func pairs(first, every, count int) []domain.OffsetFilePtr {
	var built []domain.OffsetFilePtr
	for i := 0; i < count; i++ {
		offset := first + i*every
		built = append(built, domain.OffsetFilePtr{
			Offset:     domain.Offset(offset),
			ByteOffset: int64(offset) * 7,
		})
	}
	return built
}

// The search has to answer exactly what the linear scan answered, for every query, including
// the ones that fall before the first pair, between pairs, on a pair and past the last.
func TestBinarySearchAgreesWithTheLinearScan(t *testing.T) {
	shapes := []struct {
		name  string
		pairs []domain.OffsetFilePtr
	}{
		{"empty", nil},
		{"single pair at zero", pairs(0, 1, 1)},
		{"single pair past zero", pairs(37, 1, 1)},
		{"from zero, sparsity 10", pairs(0, 10, 12)},
		{"from a block boundary", pairs(1010, 10, 12)},
		{"every offset indexed", pairs(5, 1, 30)},
		{"two pairs far apart", []domain.OffsetFilePtr{{Offset: 0, ByteOffset: 0}, {Offset: 9000, ByteOffset: 63000}}},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			idx := Index{IndexOffsets: shape.pairs}
			for query := 0; query <= 1200; query++ {
				offset := domain.Offset(query)
				got := idx.FindNearestByteOffset(offset)
				want := linearNearest(shape.pairs, offset)
				if got != want {
					t.Fatalf("offset %d: got %+v, want %+v", query, got, want)
				}
			}
		})
	}
}

// An index whose first pair is past the offset asked for has nothing to start a scan from,
// which is a zero pair and means "start at the beginning of the block". This is reachable:
// a block that does not begin on a multiple of the sparsity has no pair at its own start.
func TestOffsetBeforeTheFirstPairFindsNothing(t *testing.T) {
	idx := Index{IndexOffsets: pairs(1010, 10, 5)}
	if got := idx.FindNearestByteOffset(1009); got != (domain.OffsetFilePtr{}) {
		t.Errorf("got %+v for an offset before the first pair, want a zero pair", got)
	}
	if got := idx.FindNearestByteOffset(1010); got.Offset != 1010 {
		t.Errorf("got %+v for an offset on the first pair, want it", got)
	}
}

func TestFindOnAnEmptyIndex(t *testing.T) {
	idx := Index{}
	if got := idx.FindNearestByteOffset(42); got != (domain.OffsetFilePtr{}) {
		t.Errorf("got %+v from an empty index, want a zero pair", got)
	}
}

func BenchmarkFindNearestByteOffset(b *testing.B) {
	for _, count := range []int{16, 1024, 65536} {
		idx := Index{IndexOffsets: pairs(0, 10, count)}
		last := domain.Offset(count * 10)
		b.Run(fmt.Sprintf("pairs=%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// the worst case for a scan back from the end: the oldest offset
				idx.FindNearestByteOffset(last - domain.Offset(i%2))
				idx.FindNearestByteOffset(0)
			}
		})
	}
}

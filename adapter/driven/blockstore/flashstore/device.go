// Package flashstore is the exotic adapter the port was shaped for: a BlockStore over a
// fixed region of raw flash, the storage a microcontroller actually has. There is no
// filesystem, no directory and no growth: a fixed number of pages, bits that only go from
// one to zero until a whole page is erased, and a page table kept in RAM.
//
// It reaches nothing outside the standard library, and it passes the same conformance suite
// as the filesystem adapter, which is the only reason to trust it.
package flashstore

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrNoSpace is returned when the region has no free page left.
	ErrNoSpace = errors.New("no free flash page")

	// ErrNotErased is returned by a program that would have to set a bit back to one.
	ErrNotErased = errors.New("page is not erased")

	// ErrOutOfRange is returned for a page that is not on the device.
	ErrOutOfRange = errors.New("page is out of range")
)

// Device is the raw storage underneath: a fixed number of equally sized pages that can be
// read, programmed and erased. Programming only clears bits; setting one back needs an
// erase of the whole page.
type Device interface {
	// Pages is the number of pages the region holds.
	Pages() int
	// PageSize is the number of bytes in a page.
	PageSize() int
	// ReadPage copies a whole page into into, which must be PageSize long.
	ReadPage(page int, into []byte) error
	// ProgramPage writes data at offset within a page. Every bit it sets must already be
	// set, so a byte can be programmed again only to clear more of its bits.
	ProgramPage(page int, offset int, data []byte) error
	// ErasePage sets every byte of a page back to 0xff.
	ErasePage(page int) error
}

// RAMDevice is a Device in ordinary memory, with the same rules a flash chip has. It backs
// the tests, and a build whose "flash" is a static array.
type RAMDevice struct {
	// mu serializes access the way a single flash bus does
	mu       sync.RWMutex
	pageSize int
	pages    int
	data     []byte
	// Programs and Erases count what the device was asked to do; read them with Counters.
	Programs int
	Erases   int
}

var _ Device = &RAMDevice{}

// NewRAMDevice returns an erased region of the given shape.
func NewRAMDevice(pages int, pageSize int) *RAMDevice {
	data := make([]byte, pages*pageSize)
	for i := range data {
		data[i] = 0xff
	}
	return &RAMDevice{pageSize: pageSize, pages: pages, data: data}
}

func (d *RAMDevice) Pages() int {
	return d.pages
}

// Counters reports how often the region was programmed and erased, which is what wear
// levelling would care about.
func (d *RAMDevice) Counters() (programs int, erases int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Programs, d.Erases
}

func (d *RAMDevice) PageSize() int {
	return d.pageSize
}

func (d *RAMDevice) ReadPage(page int, into []byte) error {
	if page < 0 || page >= d.pages {
		return fmt.Errorf("%w: page %d of %d", ErrOutOfRange, page, d.pages)
	}
	if len(into) != d.pageSize {
		return fmt.Errorf("%w: buffer of %d bytes for a page of %d", ErrOutOfRange, len(into), d.pageSize)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	copy(into, d.data[page*d.pageSize:(page+1)*d.pageSize])
	return nil
}

func (d *RAMDevice) ProgramPage(page int, offset int, data []byte) error {
	if page < 0 || page >= d.pages {
		return fmt.Errorf("%w: page %d of %d", ErrOutOfRange, page, d.pages)
	}
	if offset < 0 || offset+len(data) > d.pageSize {
		return fmt.Errorf("%w: %d bytes at %d of a page of %d", ErrOutOfRange, len(data), offset, d.pageSize)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	start := page*d.pageSize + offset
	for i, b := range data {
		if b&^d.data[start+i] != 0 {
			return fmt.Errorf("%w: page %d byte %d holds %#02x, cannot hold %#02x",
				ErrNotErased, page, offset+i, d.data[start+i], b)
		}
	}
	copy(d.data[start:], data)
	d.Programs++
	return nil
}

func (d *RAMDevice) ErasePage(page int) error {
	if page < 0 || page >= d.pages {
		return fmt.Errorf("%w: page %d of %d", ErrOutOfRange, page, d.pages)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	start := page * d.pageSize
	for i := start; i < start+d.pageSize; i++ {
		d.data[i] = 0xff
	}
	d.Erases++
	return nil
}

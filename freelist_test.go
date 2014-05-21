package bolt

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a page is added to a transaction's freelist.
func TestFreelist_free(t *testing.T) {
	f := &freelist{pending: make(map[txid][]pgid)}
	f.free(100, &page{id: 12})
	assert.Equal(t, f.pending[100], []pgid{12})
}

// Ensure that a page and its overflow is added to a transaction's freelist.
func TestFreelist_free_overflow(t *testing.T) {
	f := &freelist{pending: make(map[txid][]pgid)}
	f.free(100, &page{id: 12, overflow: 3})
	assert.Equal(t, f.pending[100], []pgid{12, 13, 14, 15})
}

// Ensure that a transaction's free pages can be released.
func TestFreelist_release(t *testing.T) {
	f := &freelist{pending: make(map[txid][]pgid)}
	f.free(100, &page{id: 12, overflow: 1})
	f.free(100, &page{id: 9})
	f.free(102, &page{id: 39})
	f.release(100)
	f.release(101)
	assert.Equal(t, []pgid{9, 12, 13}, f.ids)
	f.release(102)
	assert.Equal(t, []pgid{9, 12, 13, 39}, f.ids)
}

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelist_allocate(t *testing.T) {
	f := &freelist{ids: []pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}}
	assert.Equal(t, 3, int(f.allocate(3)))
	assert.Equal(t, 6, int(f.allocate(1)))
	assert.Equal(t, 0, int(f.allocate(3)))
	assert.Equal(t, 12, int(f.allocate(2)))
	assert.Equal(t, 7, int(f.allocate(1)))
	assert.Equal(t, 0, int(f.allocate(0)))
	assert.Equal(t, []pgid{9, 18}, f.ids)
	assert.Equal(t, 9, int(f.allocate(1)))
	assert.Equal(t, 18, int(f.allocate(1)))
	assert.Equal(t, 0, int(f.allocate(1)))
	assert.Equal(t, []pgid{}, f.ids)
}

// Ensure that a freelist can deserialize from a freelist page.
func TestFreelist_read(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = freelistPageFlag
	page.count = 2

	// Insert 2 page ids.
	ids := (*[3]pgid)(unsafe.Pointer(&page.ptr))
	ids[0] = 23
	ids[1] = 50

	// Deserialize page into a freelist.
	f := &freelist{pending: make(map[txid][]pgid)}
	f.read(page)

	// Ensure that there are two page ids in the freelist.
	assert.Equal(t, len(f.ids), 2)
	assert.Equal(t, f.ids[0], pgid(23))
	assert.Equal(t, f.ids[1], pgid(50))
}

// Ensure that a freelist can serialize into a freelist page.
func TestFreelist_write(t *testing.T) {
	// Create a freelist and write it to a page.
	var buf [4096]byte
	f := &freelist{ids: []pgid{12, 39}, pending: make(map[txid][]pgid)}
	f.pending[100] = []pgid{28, 11}
	f.pending[101] = []pgid{3}
	p := (*page)(unsafe.Pointer(&buf[0]))
	f.write(p)

	// Read the page back out.
	f2 := &freelist{pending: make(map[txid][]pgid)}
	f2.read(p)

	// Ensure that the freelist is correct.
	// All pages should be present and in reverse order.
	assert.Equal(t, len(f2.ids), 5)
	assert.Equal(t, f2.ids[0], pgid(3))
	assert.Equal(t, f2.ids[1], pgid(11))
	assert.Equal(t, f2.ids[2], pgid(12))
	assert.Equal(t, f2.ids[3], pgid(28))
	assert.Equal(t, f2.ids[4], pgid(39))
}

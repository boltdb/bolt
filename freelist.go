package bolt

import (
	"errors"
	"fmt"
	"sort"
	"unsafe"
)

var (
	// ErrFreelistOverflow is returned when the total number of free pages
	// exceeds 65,536 and the freelist cannot hold any more.
	ErrFreelistOverflow = errors.New("freelist overflow")
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	ids     []pgid
	pending map[txid][]pgid
}

// size returns the size of the page after serialization.
func (f *freelist) size() int {
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * len(f.all()))
}

// all returns a list of all free ids and all pending ids in one sorted list.
func (f *freelist) all() []pgid {
	ids := make([]pgid, len(f.ids))
	copy(ids, f.ids)

	for _, list := range f.pending {
		ids = append(ids, list...)
	}

	sort.Sort(pgids(ids))
	return ids
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		_assert(id > 1, "invalid page allocation: %d", id)

		// Reset initial page if this is not contiguous.
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		if (id-initial)+1 == pgid(n) {
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}
			return initial
		}

		previd = id
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {
	_assert(p.id > 1, "cannot free page 0 or 1: %d", p.id)

	// Verify that page is not already free.
	minid, maxid := p.id, p.id+pgid(p.overflow)
	for _, id := range f.ids {
		if id >= minid && id <= maxid {
			panic(fmt.Sprintf("page %d already freed in tx", id))
		}
	}
	for ptxid, m := range f.pending {
		for _, id := range m {
			if id >= minid && id <= maxid {
				panic(fmt.Sprintf("tx %d: page %d already freed in tx %d", txid, id, ptxid))
			}
		}
	}

	// Free page and all its overflow pages.
	var ids = f.pending[txid]
	for i := 0; i < int(p.overflow+1); i++ {
		ids = append(ids, p.id+pgid(i))
	}
	f.pending[txid] = ids
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	for tid, ids := range f.pending {
		if tid <= txid {
			f.ids = append(f.ids, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(pgids(f.ids))
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	delete(f.pending, txid)
}

// isFree returns whether a given page is in the free list.
func (f *freelist) isFree(pgid pgid) bool {
	for _, id := range f.ids {
		if id == pgid {
			return true
		}
	}
	for _, m := range f.pending {
		for _, id := range m {
			if id == pgid {
				return true
			}
		}
	}
	return false
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0:p.count]
	f.ids = make([]pgid, len(ids))
	copy(f.ids, ids)
	sort.Sort(pgids(f.ids))
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.
	ids := f.all()

	// Make sure that the sum of all free pages is less than the max uint16 size.
	if len(ids) >= 65565 {
		return ErrFreelistOverflow
	}

	// Update the header and write the ids to the page.
	p.flags |= freelistPageFlag
	p.count = uint16(len(ids))
	copy(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:], ids)

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// Filter out pending free pages.
	ids := f.ids
	f.ids = nil

	var a []pgid
	for _, id := range ids {
		if !f.isFree(id) {
			a = append(a, id)
		}
	}
	f.ids = a
}

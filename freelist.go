package bolt

import (
	"sort"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	ids     []pgid
	pending map[txnid][]pgid
}

// all returns a list of all free ids and all pending ids in one sorted list.
func (f *freelist) all() []pgid {
	ids := make([]pgid, len(f.ids))
	copy(ids, f.ids)

	for _, list := range f.pending {
		ids = append(ids, list...)
	}

	sort.Sort(reverseSortedPgids(ids))
	return ids
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
func (f *freelist) allocate(n int) pgid {
	var count int
	var previd pgid
	for i, id := range f.ids {
		// Reset count if this is not contiguous.
		if previd == 0 || previd-id != 1 {
			count = 1
		}

		// If we found a contiguous block then remove it and return it.
		if count == n {
			f.ids = append(f.ids[:i-(n-1)], f.ids[i+1:]...)
			_assert(id > 1, "cannot allocate page 0 or 1: %d", id)
			return id
		}

		previd = id
		count++
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
func (f *freelist) free(txnid txnid, p *page) {
	var ids = f.pending[txnid]
	_assert(p.id > 1, "cannot free page 0 or 1: %d", p.id)
	for i := 0; i < int(p.overflow+1); i++ {
		ids = append(ids, p.id+pgid(i))
	}
	f.pending[txnid] = ids
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txnid txnid) {
	for tid, ids := range f.pending {
		if tid <= txnid {
			f.ids = append(f.ids, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(reverseSortedPgids(f.ids))
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0:p.count]
	f.ids = make([]pgid, len(ids))
	copy(f.ids, ids)
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *page) {
	ids := f.all()
	p.flags |= freelistPageFlag
	p.count = uint16(len(ids))
	copy(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:], ids)
}

type reverseSortedPgids []pgid

func (s reverseSortedPgids) Len() int           { return len(s) }
func (s reverseSortedPgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s reverseSortedPgids) Less(i, j int) bool { return s[i] > s[j] }

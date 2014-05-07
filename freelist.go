package bolt

import (
	"sort"
	"unsafe"
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
func (f *freelist) free(txid txid, p *page) {
	var ids = f.pending[txid]
	_assert(p.id > 1, "cannot free page 0 or 1: %d", p.id)
	for i := 0; i < int(p.overflow+1); i++ {
		ids = append(ids, p.id+pgid(i))
	}
	f.pending[txid] = ids

	// DEBUG ONLY: f.check()
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	for tid, ids := range f.pending {
		if tid <= txid {
			f.ids = append(f.ids, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(reverseSortedPgids(f.ids))
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

// check verifies there are no double free pages.
// This is slow so it should only be used while debugging.
// If errors are found then a panic invoked.
/*
func (f *freelist) check() {
	var lookup = make(map[pgid]txid)
	for _, id := range f.ids {
		if _, ok := lookup[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		lookup[id] = 0
	}
	for txid, m := range f.pending {
		for _, id := range m {
			if _, ok := lookup[id]; ok {
				panic(fmt.Sprintf("tx %d: page %d already freed in tx %d", txid, id, lookup[id]))
			}
			lookup[id] = txid
		}
	}
}
*/

type reverseSortedPgids []pgid

func (s reverseSortedPgids) Len() int           { return len(s) }
func (s reverseSortedPgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s reverseSortedPgids) Less(i, j int) bool { return s[i] > s[j] }

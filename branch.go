package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

// branch represents a temporary in-memory branch page.
type branch struct {
	parent *branch
	items branchItems
}

// insert inserts a new item after a given pgid.
func (b *branch) insert(key []byte, previd pgid, id pgid) {
	// Find previous insertion index.
	index := sort.Search(len(b.items), func(i int) bool { return b.items[i].pgid >= previd })

	// If there is no existing key then add a new item.
	b.items = append(b.items, branchItem{})
	if index < len(b.items) {
		copy(b.items[index+1:], b.items[index:])
	}

	b.items[index].pgid = id
	b.items[index].key = key
}

// replace swaps out an existing node id for a new one id.
func (b *branch) replace(oldid pgid, newid pgid, key []byte) {
	index := sort.Search(len(b.items), func(i int) bool { return b.items[i].pgid >= oldid })
	b.items[index].pgid = newid
	b.items[index].key = key
}

// read initializes the item data from an on-disk page.
func (b *branch) read(page *page) {
	ncount := int(page.count)
	b.items = make(branchItems, ncount)
	bnodes := (*[maxNodesPerPage]bnode)(unsafe.Pointer(&page.ptr))
	for i := 0; i < ncount; i++ {
		bnode := &bnodes[i]
		item := &b.items[i]
		item.key = bnode.key()
	}
}


type branchItems []branchItem

type branchItem struct {
	pgid pgid
	key   []byte
}

func (s branchItems) Len() int           { return len(s) }
func (s branchItems) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s branchItems) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }


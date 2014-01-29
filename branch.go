package bolt

import (
	"bytes"
	"unsafe"
)

// branch represents a temporary in-memory branch page.
type branch struct {
	parent *branch
	items branchItems
}

// put adds a new node or replaces an existing node.
func (b *branch) put(id pgid, newid pgid, key []byte, replace bool) {
	var index int
	for ; index < len(b.items); index++ {
		if b.items[index].pgid == id {
			break
		}
	}

	if !replace {
		index++
		b.items = append(b.items, branchItem{})
		if index < len(b.items) {
			copy(b.items[index+1:], b.items[index:])
		}
	}

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


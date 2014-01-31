package bolt

import (
	"bytes"
	"unsafe"
)

// branch represents a temporary in-memory branch page.
type branch struct {
	pgid   pgid
	depth  int
	parent *branch
	items  branchItems
}

// size returns the size of the branch after serialization.
func (b *branch) size() int {
	var size int = pageHeaderSize
	for _, item := range b.items {
		size += bnodeSize + len(item.key)
	}
	return size
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
func (b *branch) read(p *page) {
	b.pgid = p.id
	b.items = make(branchItems, int(p.count))
	bnodes := (*[maxNodesPerPage]bnode)(unsafe.Pointer(&p.ptr))
	for i := 0; i < int(p.count); i++ {
		bnode := &bnodes[i]
		item := &b.items[i]
		item.pgid = bnode.pgid
		item.key = bnode.key()
	}
}

// write writes the items onto a branch page.
func (b *branch) write(p *page) {
	// Initialize page.
	p.flags |= p_branch
	p.count = uint16(len(b.items))

	// Loop over each item and write it to the page.
	bnodes := (*[maxNodesPerPage]bnode)(unsafe.Pointer(&p.ptr))
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[lnodeSize*len(b.items):]
	for index, item := range b.items {
		// Write node.
		bnode := &bnodes[index]
		bnode.pgid = item.pgid
		bnode.pos = uint32(uintptr(unsafe.Pointer(&buf[0])) - uintptr(unsafe.Pointer(bnode)))
		bnode.ksize = uint32(len(item.key))

		// Write key to the end of the page.
		copy(buf[0:], item.key)
		buf = buf[len(item.key):]
	}
}

// split divides up the noes in the branch into appropriately sized groups.
func (b *branch) split(pageSize int) []*branch {
	// Ignore the split if the page doesn't have at least enough nodes for
	// multiple pages or if the data can fit on a single page.
	if len(b.items) <= (minKeysPerPage*2) || b.size() < pageSize {
		return []*branch{b}
	}

	// Set fill threshold to 50%.
	threshold := pageSize / 2

	// Otherwise group into smaller pages and target a given fill size.
	size := 0
	current := &branch{}
	branches := make([]*branch, 0)

	for index, item := range b.items {
		nodeSize := bnodeSize + len(item.key)

		if len(current.items) >= minKeysPerPage && index < len(b.items)-minKeysPerPage && size+nodeSize > threshold {
			size = pageHeaderSize
			branches = append(branches, current)
			current = &branch{}
		}

		size += nodeSize
		current.items = append(current.items, item)
	}
	branches = append(branches, current)

	return branches
}

type branches []*branch

func (s branches) Len() int           { return len(s) }
func (s branches) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s branches) Less(i, j int) bool { return s[i].depth < s[j].depth }

type branchItems []branchItem

type branchItem struct {
	pgid pgid
	key  []byte
}

func (s branchItems) Len() int           { return len(s) }
func (s branchItems) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s branchItems) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }

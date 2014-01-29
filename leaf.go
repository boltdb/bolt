package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

// leaf represents a temporary in-memory leaf page.
// It is deserialized from an memory-mapped page and is not restricted by page size.
type leaf struct {
	parent *branch
	items leafItems
}

// put inserts or replaces a key on a leaf page.
func (l *leaf) put(key []byte, value []byte) {
	// Find insertion index.
	index := sort.Search(len(l.items), func(i int) bool { return bytes.Compare(l.items[i].key, key) != -1 })

	// If there is no existing key then add a new item.
	if index == len(l.items) {
		l.items = append(l.items, leafItem{})
	} else if len(l.items) == 0 || !bytes.Equal(l.items[index].key, key) {
		l.items = append(l.items, leafItem{})
		copy(l.items[index+1:], l.items[index:])
	}
	l.items[index].key = key
	l.items[index].value = value
}

// size returns the size of the leaf after serialization.
func (l *leaf) size() int {
	var size int = pageHeaderSize
	for _, item := range l.items {
		size += lnodeSize + len(item.key) + len(item.value)
	}
	return size
}

// read initializes the item data from an on-disk page.
func (l *leaf) read(p *page) {
	ncount := int(p.count)
	l.items = make(leafItems, ncount)
	lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&p.ptr))
	for i := 0; i < ncount; i++ {
		lnode := &lnodes[i]
		item := &l.items[i]
		item.key = lnode.key()
		item.value = lnode.value()
	}
}

// write writes the items onto one or more leaf pages.
func (l *leaf) write(p *page) {
	// Initialize page.
	p.flags |= p_leaf
	p.count = uint16(len(l.items))

	// Loop over each item and write it to the page.
	lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&p.ptr))
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[lnodeSize*len(l.items):]
	for index, item := range l.items {
		// Write node.
		lnode := &lnodes[index]
		lnode.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(lnode)))
		lnode.ksize = uint32(len(item.key))
		lnode.vsize = uint32(len(item.value))

		// Write data to the end of the page.
		copy(b[0:], item.key)
		b = b[len(item.key):]
		copy(b[0:], item.value)
		b = b[len(item.value):]
	}
}

// split divides up the noes in the page into appropriately sized groups.
func (l *leaf) split(pageSize int) []*leaf {
	// Ignore the split if the page doesn't have at least enough nodes for
	// multiple pages or if the data can fit on a single page.
	if len(l.items) <= (minKeysPerPage * 2) || l.size() < pageSize {
		return []*leaf{l}
	}

	// Set fill threshold to 25%.
	threshold := pageSize >> 4

	// Otherwise group into smaller pages and target a given fill size.
	size := 0
	current := &leaf{}
	leafs := make([]*leaf, 0)

	for index, item := range l.items {
		nodeSize := lnodeSize + len(item.key) + len(item.value)

		if (len(current.items) >= minKeysPerPage && index < len(l.items)-minKeysPerPage && size+nodeSize > threshold) {
			size = pageHeaderSize
			leafs = append(leafs, current)
			current = &leaf{}
		}

		size += nodeSize
		current.items = append(current.items, item)
	}
	leafs = append(leafs, current)

	return leafs
}

type leafItems []leafItem

type leafItem struct {
	key   []byte
	value []byte
}

func (s leafItems) Len() int           { return len(s) }
func (s leafItems) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s leafItems) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }


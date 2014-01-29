package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

// leaf represents a temporary in-memory leaf page.
// It is deserialized from an memory-mapped page and is not restricted by page size.
type leaf struct {
	items leafItems
}

type leafItems []leafItem

type leafItem struct {
	key   []byte
	value []byte
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

// read initializes the item data from an on-disk page.
func (l *leaf) read(page *page) {
	ncount := int(page.count)
	l.items = make(leafItems, ncount)
	lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&page.ptr))
	for i := 0; i < ncount; i++ {
		lnode := &lnodes[i]
		item := &l.items[i]
		item.key = lnode.key()
		item.value = lnode.value()
	}
}

// write writes the items onto one or more leaf pages.
func (l *leaf) write(pageSize int, allocate func(size int) (*page, error)) ([]*page, error) {
	var pages []*page

	for _, items := range l.split(pageSize) {
		// Determine the total page size.
		var size int = pageHeaderSize
		for _, item := range l.items {
			size += lnodeSize + len(item.key) + len(item.value)
		}

		// Allocate pages.
		page, err := allocate(size)
		if err != nil {
			return nil, err
		}
		page.flags |= p_leaf
		page.count = uint16(len(items))

		// Loop over each item and write it to the page.
		lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&page.ptr))
		b := (*[maxAllocSize]byte)(unsafe.Pointer(&page.ptr))[lnodeSize*len(items):]
		for index, item := range items {
			// Write item.
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

		pages = append(pages, page)
	}

	return pages, nil
}

// split divides up the noes in the page into appropriately sized groups.
func (l *leaf) split(pageSize int) []leafItems {
	// If we don't have enough items for multiple pages then just return the items.
	if len(l.items) <= (minKeysPerPage * 2) {
		return []leafItems{l.items}
	}

	// If we're not larger than one page then just return the items.
	var totalSize int = pageHeaderSize
	for _, item := range l.items {
		totalSize += lnodeSize + len(item.key) + len(item.value)
	}
	if totalSize < pageSize {
		return []leafItems{l.items}
	}

	// Otherwise group into smaller pages and target a given fill size.
	var size int
	var group leafItems
	var groups []leafItems

	// Set fill threshold to 25%.
	threshold := pageSize >> 4

	for index, item := range l.items {
		nodeSize := lnodeSize + len(item.key) + len(item.value)

		if group == nil || (len(group) >= minKeysPerPage && index < len(l.items)-minKeysPerPage && size+nodeSize > threshold) {
			size = pageHeaderSize
			if group != nil {
				groups = append(groups, group)
			}
			group = make(leafItems, 0)
		}

		size += nodeSize
		group = append(group, item)
	}
	groups = append(groups, group)

	return groups
}

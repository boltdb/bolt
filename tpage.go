package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

// tpage represents a temporary, in-memory leaf page.
// It is deserialized from an memory-mapped page and is not restricted by page size.
type tpage struct {
	nodes tnodes
}

// allocator is a function that returns a set of contiguous pages.
type allocator func(size int) (*page, error)

// put inserts or replaces a key on a leaf page.
func (p *tpage) put(key []byte, value []byte) {
	// Find insertion index.
	index := sort.Search(len(p.nodes), func(i int) bool { return bytes.Compare(p.nodes[i].key, key) != -1 })

	// If there is no existing key then add a new node.
	if index == len(p.nodes) {
		p.nodes = append(p.nodes, tnode{})
	} else if len(p.nodes) == 0 || !bytes.Equal(p.nodes[index].key, key) {
		p.nodes = append(p.nodes, tnode{})
		copy(p.nodes[index+1:], p.nodes[index:])
	}
	p.nodes[index].key = key
	p.nodes[index].value = value
}

// read initializes the node data from an on-disk page.
func (p *tpage) read(page *page) {
	ncount := int(page.count)
	p.nodes = make(tnodes, ncount)
	lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&page.ptr))
	for i := 0; i < ncount; i++ {
		lnode := &lnodes[i]
		n := &p.nodes[i]
		n.key = lnode.key()
		n.value = lnode.value()
	}
}

// write writes the nodes onto one or more leaf pages.
func (p *tpage) write(pageSize int, allocate allocator) ([]*page, error) {
	var pages []*page

	for _, nodes := range p.split(pageSize) {
		// Determine the total page size.
		var size int = pageHeaderSize
		for _, node := range p.nodes {
			size += lnodeSize + len(node.key) + len(node.value)
		}

		// Allocate pages.
		page, err := allocate(size)
		if err != nil {
			return nil, err
		}
		page.flags |= p_leaf
		page.count = uint16(len(nodes))

		// Loop over each node and write it to the page.
		lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&page.ptr))
		b := (*[maxAllocSize]byte)(unsafe.Pointer(&page.ptr))[lnodeSize*len(nodes):]
		for index, node := range nodes {
			// Write node.
			lnode := &lnodes[index]
			lnode.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(lnode)))
			lnode.ksize = uint32(len(node.key))
			lnode.vsize = uint32(len(node.value))

			// Write data to the end of the node.
			copy(b[0:], node.key)
			b = b[len(node.key):]
			copy(b[0:], node.value)
			b = b[len(node.value):]
		}

		pages = append(pages, page)
	}

	return pages, nil
}

// split divides up the noes in the page into appropriately sized groups.
func (p *tpage) split(pageSize int) []tnodes {
	// If we only have enough nodes for one page then just return the nodes.
	if len(p.nodes) <= minKeysPerPage {
		return []tnodes{p.nodes}
	}

	// If we're not larger than one page then just return the nodes.
	var totalSize int = pageHeaderSize
	for _, node := range p.nodes {
		totalSize += lnodeSize + len(node.key) + len(node.value)
	}
	if totalSize < pageSize {
		return []tnodes{p.nodes}
	}

	// Otherwise group into smaller pages and target a given fill size.
	var size int
	var group tnodes
	var groups []tnodes

	// Set fill threshold to 25%.
	threshold := pageSize >> 4

	for _, node := range p.nodes {
		nodeSize := lnodeSize + len(node.key) + len(node.value)

		// TODO(benbjohnson): Don't create a new group for just the last node.
		if group == nil || (len(group) > minKeysPerPage && size+nodeSize > threshold) {
			size = pageHeaderSize
			group = make(tnodes, 0)
			groups = append(groups, group)
		}

		size += nodeSize
		group = append(group, node)
	}

	return groups
}

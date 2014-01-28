package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

type lpage struct {
	nodes []lpnode
}

type lpnode struct {
	key   []byte
	value []byte
}

// allocator is a function that returns a set of contiguous pages.
type allocator func(count int) (*page, error)

// put inserts or replaces a key on a leaf page.
func (p *lpage) put(key []byte, value []byte) {
	// Find insertion index.
	index := sort.Search(len(p.nodes), func(i int) bool { return bytes.Compare(p.nodes[i].key, key) != -1 })

	// If there is no existing key then add a new node.
	if len(p.nodes) == 0 || !bytes.Equal(p.nodes[index].key, key) {
		p.nodes = append(p.nodes, lpnode{})
		copy(p.nodes[index+1:], p.nodes[index:])
	}
	p.nodes[index].key = key
	p.nodes[index].value = value
}

// read initializes the node data from an on-disk page.
func (p *lpage) read(page *page) {
	p.nodes = make([]lpnode, page.count)
	lnodes := (*[maxNodesPerPage]lnode)(unsafe.Pointer(&page.ptr))
	for i := 0; i < int(page.count); i++ {
		lnode := lnodes[i]
		n := &p.nodes[i]
		n.key = lnode.key()
		n.value = lnode.value()
	}
}

// write writes the nodes onto one or more leaf pages.
func (p *lpage) write(pageSize int, allocate allocator) ([]*page, error) {
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
		b := (*[maxPageAllocSize]byte)(unsafe.Pointer(&page.ptr))[lnodeSize*len(nodes):]
		for index, node := range nodes {
			// Write node.
			lnode := &lnodes[index]
			lnode.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(&lnode)))
			lnode.ksize = uint32(len(node.key))
			lnode.vsize = uint32(len(node.value))

			// Write data to the end of the node.
			copy(b[:], node.key)
			b = b[len(node.key):]
			copy(b[:], node.value)
			b = b[len(node.value):]
		}

		pages = append(pages, page)
	}

	return pages, nil
}

// split divides up the noes in the page into appropriately sized groups.
func (p *lpage) split(pageSize int) [][]lpnode {
	// If we only have enough nodes for one page then just return the nodes.
	if len(p.nodes) <= minKeysPerPage {
		return [][]lpnode{p.nodes}
	}

	// If we're not larger than one page then just return the nodes.
	var totalSize int = pageHeaderSize
	for _, node := range p.nodes {
		totalSize += lnodeSize + len(node.key) + len(node.value)
	}
	if totalSize < pageSize {
		return [][]lpnode{p.nodes}
	}

	// Otherwise group into smaller pages and target a given fill size.
	var size int
	var group []lpnode
	var groups [][]lpnode

	// Set fill threshold to 25%.
	threshold := pageSize >> 4

	for _, node := range p.nodes {
		nodeSize := lnodeSize + len(node.key) + len(node.value)

		// TODO(benbjohnson): Don't create a new group for just the last node.
		if group == nil || (len(group) > minKeysPerPage && size+nodeSize > threshold) {
			size = pageHeaderSize
			group = make([]lpnode, 0)
			groups = append(groups, group)
		}

		size += nodeSize
		group = append(group, node)
	}

	return groups
}

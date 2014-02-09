package bolt

import (
	"bytes"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	transaction *RWTransaction
	isLeaf bool
	unbalanced bool
	key    []byte
	depth  int
	pgid   pgid
	parent *node
	inodes inodes
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the size of the node after serialization.
func (n *node) size() int {
	var elementSize int = n.pageElementSize()

	var size int = pageHeaderSize
	for _, item := range n.inodes {
		size += elementSize + len(item.key) + len(item.value)
	}
	return size
}

// pageElementSize returns the size of each page element based on the type of node.
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// root returns the root node in the tree.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {
	_assert(!n.isLeaf, "invalid childAt(%d) on a leaf node", index)
	return n.transaction.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren() - 1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.
func (n *node) put(oldKey, newKey, value []byte, pgid pgid) {
	// Find insertion index.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	if !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// read initializes the node from a page.
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & p_leaf) != 0)
	n.inodes = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
	}

	// Save first key so we can find the node in the parent when we spill.
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
func (n *node) write(p *page) {
	// Initialize page.
	if n.isLeaf {
		p.flags |= p_leaf
	} else {
		p.flags |= p_branch
	}
	p.count = uint16(len(n.inodes))

	// Loop over each item and write it to the page.
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		// Write the page element.
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
		}

		// Write data for the element to the end of the page.
		copy(b[0:], item.key)
		b = b[len(item.key):]
		copy(b[0:], item.value)
		b = b[len(item.value):]
	}
}

// split divides up the node into appropriately sized nodes.
func (n *node) split(pageSize int) []*node {
	// Ignore the split if the page doesn't have at least enough nodes for
	// multiple pages or if the data can fit on a single page.
	if len(n.inodes) <= (minKeysPerPage*2) || n.size() < pageSize {
		return []*node{n}
	}

	// Set fill threshold to 50%.
	threshold := pageSize / 2

	// Group into smaller pages and target a given fill size.
	size := pageHeaderSize
	inodes := n.inodes
	current := n
	current.inodes = nil
	var nodes []*node

	for i, inode := range inodes {
		elemSize := n.pageElementSize() + len(inode.key) + len(inode.value)

		if len(current.inodes) >= minKeysPerPage && i < len(inodes)-minKeysPerPage && size+elemSize > threshold {
			size = pageHeaderSize
			nodes = append(nodes, current)
			current = &node{transaction: n.transaction, isLeaf: n.isLeaf}
		}

		size += elemSize
		current.inodes = append(current.inodes, inode)
	}
	nodes = append(nodes, current)

	return nodes
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Ignore if node is above threshold (25%) and has enough keys.
	var threshold = n.transaction.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move child's children up.
			child := n.transaction.nodes[n.inodes[0].pgid]
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]

			// Reparent all child nodes being moved.
			for _, inode := range n.inodes {
				if child, ok := n.transaction.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			delete(n.transaction.nodes, child.pgid)
		}

		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// If target node has extra nodes then just move one over.
	if target.numChildren() > target.minKeys() {
		if useNextSibling {
			// Reparent and move node.
			if child, ok := n.transaction.nodes[target.inodes[0].pgid]; ok {
				child.parent = n
			}
			n.inodes = append(n.inodes, target.inodes[0])
			target.inodes = target.inodes[1:]

			// Update target key on parent.
			target.parent.put(target.key, target.inodes[0].key, nil, target.pgid)
			target.key = target.inodes[0].key
		} else {
			// Reparent and move node.
			if child, ok := n.transaction.nodes[target.inodes[len(target.inodes)-1].pgid]; ok {
				child.parent = n
			}
			n.inodes = append(n.inodes, inode{})
			copy(n.inodes[1:], n.inodes)
			n.inodes[0] = target.inodes[len(target.inodes)-1]
			target.inodes = target.inodes[:len(target.inodes)-1]
		}

		// Update parent key for node.
		n.parent.put(n.key, n.inodes[0].key, nil, n.pgid)
		n.key = n.inodes[0].key

		return
	}

	// If both this node and the target node are too small then merge them.
	if useNextSibling {
		// Reparent all child nodes being moved.
		for _, inode := range target.inodes {
			if child, ok := n.transaction.nodes[inode.pgid]; ok {
				child.parent = n
			}
		}

		// Copy over inodes from target and remove target.
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		delete(n.transaction.nodes, target.pgid)
	} else {
		// Reparent all child nodes being moved.
		for _, inode := range n.inodes {
			if child, ok := n.transaction.nodes[inode.pgid]; ok {
				child.parent = target
			}
		}

		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.put(target.key, target.inodes[0].key, nil, target.pgid)
		delete(n.transaction.nodes, n.pgid)
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	n.parent.rebalance()
}

// nodesByDepth sorts a list of branches by deepest first.
type nodesByDepth []*node

func (s nodesByDepth) Len() int           { return len(s) }
func (s nodesByDepth) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodesByDepth) Less(i, j int) bool { return s[i].depth > s[j].depth }

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	pgid  pgid
	key   []byte
	value []byte
}

type inodes []inode

func (s inodes) Len() int           { return len(s) }
func (s inodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s inodes) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }

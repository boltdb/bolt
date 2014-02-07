package bolt

import (
	"bytes"
	"sort"
)

type Cursor struct {
	transaction *Transaction
	root        pgid
	stack       []pageElementRef
}

// First moves the cursor to the first item in the bucket and returns its key and data.
func (c *Cursor) First() ([]byte, []byte) {
	// TODO: Traverse to the first key.
	return nil, nil
}

// Move the cursor to the next key/value.
func (c *Cursor) Next() ([]byte, []byte) {
	return nil, nil
}

// Get positions the cursor at a specific key and returns the its value.
func (c *Cursor) Get(key []byte) []byte {
	// Start from root page and traverse to correct page.
	c.stack = c.stack[:0]
	c.search(key, c.transaction.page(c.root))
	p, index := c.top()

	// If the cursor is pointing to the end of page then return nil.
	if index == p.count {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, c.element().key()) {
		return nil
	}

	return c.element().value()
}

func (c *Cursor) search(key []byte, p *page) {
	if (p.flags & (p_branch | p_leaf)) == 0 {
		panic("invalid page type: " + p.typ())
	}
	e := pageElementRef{page: p}
	c.stack = append(c.stack, e)

	// If we're on a leaf page then find the specific node.
	if (p.flags & p_leaf) != 0 {
		c.nsearch(key, p)
		return
	}

	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = uint16(index)

	// Recursively search to the next page.
	c.search(key, c.transaction.page(inodes[index].pgid))
}

// nsearch searches a leaf node for the index of the node that matches key.
func (c *Cursor) nsearch(key []byte, p *page) {
	e := &c.stack[len(c.stack)-1]

	// Binary search for the correct leaf node index.
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = uint16(index)
}

// top returns the page and leaf node that the cursor is currently pointing at.
func (c *Cursor) top() (*page, uint16) {
	ptr := c.stack[len(c.stack)-1]
	return ptr.page, ptr.index
}

// page returns the page that the cursor is currently pointing at.
func (c *Cursor) page() *page {
	return c.stack[len(c.stack)-1].page
}

// element returns the leaf element that the cursor is currently positioned on.
func (c *Cursor) element() *leafPageElement {
	ref := c.stack[len(c.stack)-1]
	return ref.page.leafPageElement(ref.index)
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node(t *RWTransaction) *node {
	if len(c.stack) == 0 {
		return nil
	}

	// Start from root and traverse down the hierarchy.
	n := t.node(c.stack[0].page.id, nil)
	for _, ref := range c.stack[:len(c.stack)-1] {
		__assert__(!n.isLeaf, "expected branch node")
		__assert__(ref.page.id == n.pgid, "node/page mismatch a: %d != %d", ref.page.id, n.childAt(ref.index).pgid)
		n = n.childAt(ref.index)
	}
	__assert__(n.isLeaf, "expected leaf node")
	__assert__(n.pgid == c.stack[len(c.stack)-1].page.id, "node/page mismatch b: %d != %d", n.pgid, c.stack[len(c.stack)-1].page.id)
	return n
}


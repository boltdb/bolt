package bolt

import (
	"bytes"
	"sort"
)

type Cursor struct {
	transaction *Transaction
	root        pgid
	stack       []elem
}

// elem represents a node on a page that's on the cursor's stack.
type elem struct {
	page  *page
	index int
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
	if c.Goto(key) {
		return c.node().value()
	}
	return nil
}

// Goto positions the cursor at a specific key.
// Returns true if an exact match or false if positioned after the closest match.
func (c *Cursor) Goto(key []byte) bool {
	// TODO(benbjohnson): Optimize for specific use cases.

	// Start from root page and traverse to correct page.
	c.stack = c.stack[:0]
	c.search(key, c.transaction.page(c.root))

	return false
}

func (c *Cursor) search(key []byte, p *page) {
	e := elem{page: p}
	c.stack = append(c.stack, e)

	// If we're on a leaf page then find the specific node.
	if (p.flags & p_leaf) != 0 {
		c.nsearch(key, p)
		return
	}

	// Binary search for the correct branch node.
	nodes := p.bnodes()
	e.index = sort.Search(int(p.count)-1, func(i int) bool { return bytes.Compare(nodes[i+1].key(), key) != -1 })

	// Recursively search to the next page.
	c.search(key, c.transaction.page(nodes[e.index].pgid))
}

// nsearch searches a leaf node for the index of the node that matches key.
func (c *Cursor) nsearch(key []byte, p *page) {
	e := &c.stack[len(c.stack)-1]

	// Binary search for the correct leaf node index.
	nodes := p.lnodes()
	e.index = sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(nodes[i].key(), key) != -1
	})
}

// top returns the page and leaf node that the cursor is currently pointing at.
func (c *Cursor) top() (*page, *lnode) {
	elem := c.stack[len(c.stack)-1]
	return elem.page, elem.page.lnode(elem.index)
}

// page returns the page that the cursor is currently pointing at.
func (c *Cursor) page() *page {
	return c.stack[len(c.stack)-1].page
}

// node returns the leaf node that the cursor is currently positioned on.
func (c *Cursor) node() *lnode {
	elem := c.stack[len(c.stack)-1]
	return elem.page.lnode(elem.index)
}

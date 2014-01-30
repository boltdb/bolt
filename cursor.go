package bolt

type Cursor struct {
	bucket *Bucket
	stack  []elem
}

// elem represents a node on a page that's on the cursor's stack.
type elem struct {
	page  *page
	index int
}

func (c *Cursor) Bucket() *Bucket {
	return c.bucket
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

	// TODO: Start from root page and traverse to correct page.

	return false
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

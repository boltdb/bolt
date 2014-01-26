package bolt

type Cursor struct {
	bucket      *Bucket
	stack       []elem
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
func (c *Cursor) First() ([]byte, []byte, error) {
	// TODO: Traverse to the first key.
	return nil, nil, nil
}

// Move the cursor to the next key/value.
func (c *Cursor) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

// Goto positions the cursor at a specific key.
func (c *Cursor) Goto(key []byte) ([]byte, error) {
	// TODO(benbjohnson): Optimize for specific use cases.

	// TODO: Check if len(key) > 0.
	// TODO: Start from root page and traverse to correct page.

	return nil, nil
}

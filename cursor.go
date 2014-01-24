package bolt

type Cursor struct {
	transaction *Transaction
	bucket      *Bucket
	stack       []stackelem
}

type stackelem struct {
	page  *page
	index int
}

func (c *Cursor) Transaction() *Transaction {
	return c.transaction
}

func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

func (c *Cursor) Get(key []byte) ([]byte, error) {
	// TODO: Move to key
	// TODO: If it doesn't exist, return nil, nil
	// TODO: Otherwise return node key+data.
	return nil, nil
}

// Move the cursor to the next key/value.
func (c *Cursor) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

// First moves the cursor to the first item in the bucket and returns its key and data.
func (c *Cursor) First() ([]byte, []byte, error) {
	// TODO: Traverse to the first key.
	return nil, nil, nil
}

// Set the cursor on a specific data item.
// (bool return is whether it is exact).
func (c *Cursor) set(key []byte, data []byte, op int) (error, bool) {
	// TODO(benbjohnson): Optimize for specific use cases.

	// TODO: Check if len(key) > 0.
	// TODO: Start from root page and traverse to correct page.

	return nil, false
}

func (c *Cursor) insert(key []byte, data []byte) error {
	// TODO: If there is not enough space on page for key+data then split.
	// TODO: Move remaining data on page forward.
	// TODO: Write leaf node to current location.	
	// TODO: Adjust available page size.
	return nil
}

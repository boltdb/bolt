package bolt

// RWCursor represents a cursor that can read and write data for a bucket.
type RWCursor struct {
	Cursor
	transaction *RWTransaction
}

func (c *RWCursor) Put(key []byte, value []byte) error {
	// Make sure this cursor was created by a transaction.
	if c.transaction == nil {
		return &Error{"invalid cursor", nil}
	}
	db := c.transaction.db

	// Validate the key we're using.
	if key == nil {
		return &Error{"key required", nil}
	} else if len(key) > db.maxKeySize {
		return &Error{"key too large", nil}
	}

	// TODO: Validate data size based on MaxKeySize if DUPSORT.

	// Validate the size of our data.
	if len(data) > MaxDataSize {
		return &Error{"data too large", nil}
	}

	// If we don't have a root page then add one.
	if c.bucket.root == p_invalid {
		p, err := c.newLeafPage()
		if err != nil {
			return err
		}
		c.push(p)
		c.bucket.root = p.id
		c.bucket.root++
		// TODO: *mc->mc_dbflag |= DB_DIRTY;
		// TODO? mc->mc_flags |= C_INITIALIZED;
	}

	// TODO: Move to key.
	exists, err := c.moveTo(key)
	if err != nil {
		return err
	}

	// TODO: spill?
	if err := c.spill(key, data); err != nil {
		return err
	}

	// Make sure all cursor pages are writable
	if err := c.touch(); err != nil {
		return err
	}

	// If key does not exist the
	if exists {
		node := c.currentNode()

	}

	return nil
}

func (c *Cursor) Delete(key []byte) error {
	// TODO: Traverse to the correct node.
	// TODO: If missing, exit.
	// TODO: Remove node from page.
	// TODO: If page is empty then add it to the freelist.
	return nil
}

// newLeafPage allocates and initialize new a new leaf page.
func (c *RWCursor) newLeafPage() (*page, error) {
	// Allocate page.
	p, err := c.allocatePage(1)
	if err != nil {
		return nil, err
	}

	// Set flags and bounds.
	p.flags = p_leaf | p_dirty
	p.lower = pageHeaderSize
	p.upper = c.transaction.db.pageSize

	return p, nil
}

// newBranchPage allocates and initialize new a new branch page.
func (b *RWCursor) newBranchPage() (*page, error) {
	// Allocate page.
	p, err := c.allocatePage(1)
	if err != nil {
		return nil, err
	}

	// Set flags and bounds.
	p.flags = p_branch | p_dirty
	p.lower = pageHeaderSize
	p.upper = c.transaction.db.pageSize

	return p, nil
}

package bolt

import (
	"unsafe"
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction
}

// TODO: Allocate scratch meta page.
// TODO: Allocate scratch data pages.
// TODO: Track dirty pages (?)

func (t *RWTransaction) Commit() error {
	// TODO: Update non-system bucket pointers.
	// TODO: Save freelist.
	// TODO: Flush data.

	// TODO: Initialize new meta object, Update system bucket nodes, last pgno, txnid.
	// meta.mm_dbs[0] = txn->mt_dbs[0];
	// meta.mm_dbs[1] = txn->mt_dbs[1];
	// meta.mm_last_pg = txn->mt_next_pgno - 1;
	// meta.mm_txnid = txn->mt_txnid;

	// TODO: Pick sync or async file descriptor.
	// TODO: Write meta page to file.

	// TODO(?): Write checksum at the end.

	return nil
}

func (t *RWTransaction) Rollback() error {
	return t.close()
}

func (t *RWTransaction) close() error {
	// TODO: Free scratch pages.
	// TODO: Release writer lock.
	return nil
}

// CreateBucket creates a new bucket.
func (t *RWTransaction) CreateBucket(name string) error {
	if t.db == nil {
		return InvalidTransactionError
	}

	// Check if bucket already exists.
	if b, err := t.Bucket(name); err != nil {
		return err
	} else if b != nil {
		return &Error{"bucket already exists", nil}
	}

	// Create a new bucket entry.
	var buf [unsafe.Sizeof(bucket{})]byte
	var raw = (*bucket)(unsafe.Pointer(&buf[0]))
	raw.root = 0

	// Open cursor to system bucket.
	c := t.sys.cursor()
	if c.Goto([]byte(name)) {
		// TODO: Delete node first.
	}

	// Insert new node.
	if err := t.insert([]byte(name), buf[:]); err != nil {
		return err
	}

	return nil
}

// DropBucket deletes a bucket.
func (t *RWTransaction) DeleteBucket(b *Bucket) error {
	// TODO: Remove from main DB.
	// TODO: Delete entry from system bucket.
	// TODO: Free all pages.
	// TODO: Remove cursor.
	return nil
}

// Flush (some) dirty pages to the map, after clearing their dirty flag.
// @param[in] txn the transaction that's being committed
// @param[in] keep number of initial pages in dirty_list to keep dirty.
// @return 0 on success, non-zero on failure.
func (t *RWTransaction) flush(keep bool) error {
	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// TODO: Loop over each dirty page and write it to disk.
	return nil
}

func (t *RWTransaction) Put(name string, key []byte, value []byte) error {
	b := t.Bucket(name)
	if b == nil {
		return BucketNotFoundError
	}

	// Validate the key and data size.
	if len(key) == 0 {
		return &Error{"key required", nil}
	} else if len(key) > MaxKeySize {
		return &Error{"key too large", nil}
	} else if len(value) > MaxDataSize {
		return &Error{"data too large", nil}
	}

	// Move cursor to insertion position.
	c := b.cursor()
	replace := c.Goto()
	p, index := c.current()

	// Insert a new node.
	if err := t.insert(p, index, key, value, replace); err != nil {
		return err
	}

	return nil
}

func (t *RWTransaction) Delete(key []byte) error {
	// TODO: Traverse to the correct node.
	// TODO: If missing, exit.
	// TODO: Remove node from page.
	// TODO: If page is empty then add it to the freelist.
	return nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(count int) (*page, error) {
	// TODO: Find a continuous block of free pages.
	// TODO: If no free pages are available, resize the mmap to allocate more.
	return nil, nil
}

func (t *RWTransaction) insert(p *page, index int, key []byte, data []byte, replace bool) error {
	nodes := copy(p.lnodes())
	if replace {
		nodes = nodes.replace(index, key, data)
	} else {
		nodes = nodes.insert(index, key, data)
	}

	// If our page fits in the same size page then just write it.
	if pageHeaderSize + nodes.size() < p.size() {
		// TODO: Write new page.
		// TODO: Update parent branches.
	}

	// Calculate total page size.
	size := pageHeaderSize
	for _, n := range nodes {
		size += lnodeSize + n.ksize + n.vsize
	}

	// If our new page fits in our current page size then just write it.
	if size < t.db.pageSize {

		return t.writeLeafPage(p.id, nodes)
	}

	var nodesets [][]lnodes
	if size < t.db.pageSize {
		nodesets = [][]lnodes{nodes}
	}

	nodesets := t.split(nodes)

	// TODO: Move remaining data on page forward.
	// TODO: Write leaf node to current location.	
	// TODO: Adjust available page size.
	return nil
}

// split takes a list of nodes and returns multiple sets of nodes if a
// page split is required.
func (t *RWTransaction) split(nodes []lnodes) [][]lnodes {

	// If the size is less than the page size then just return the current set.
	if size < t.db.pageSize {
		return [][]lnodes{nodes}
	}

	// Otherwise loop over nodes and split up into multiple pages.
	var nodeset []lnodes
	var nodesets [][]lnodes
	for _, n := range nodes {

	}

}

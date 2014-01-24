package bolt

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction

	dirtyPages map[int]*page
	freelist   []pgno
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
func (t *RWTransaction) CreateBucket(name string, dupsort bool) (*Bucket, error) {
	if t.db == nil {
		return nil, InvalidTransactionError
	}

	// Check if bucket already exists.
	if b := t.buckets[name]; b != nil {
		return nil, &Error{"bucket already exists", nil}
	}

	// Create a new bucket entry.
	var buf [unsafe.Sizeof(bucket{})]byte
	var raw = (*bucket)(unsafe.Pointer(&buf[0]))
	raw.root = p_invalid
	// TODO: Set dupsort flag.

	// Open cursor to system bucket.
	c, err := t.Cursor(&t.sysbuckets)
	if err != nil {
		return nil, err
	}

	// Put new entry into system bucket.
	if err := c.Put([]byte(name), buf[:]); err != nil {
		return nil, err
	}

	// Save reference to bucket.
	b := &Bucket{name: name, bucket: raw, isNew: true}
	t.buckets[name] = b

	// TODO: dbflag |= DB_DIRTY;

	return b, nil
}

// DropBucket deletes a bucket.
func (t *RWTransaction) DeleteBucket(b *Bucket) error {
	// TODO: Find bucket.
	// TODO: Remove entry from system bucket.
	return nil
}

// Put sets the value for a key in a given bucket.
func (t *Transaction) Put(name string, key []byte, value []byte) error {
	c, err := t.Cursor(name)
	if err != nil {
		return nil, err
	}
	return c.Put(key, value)
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Transaction) page(id int) *page {
	// Check the dirty pages first.
	if p, ok := t.pages[id]; ok {
		return p
	}

	// Otherwise return directly from the mmap.
	return t.Transaction.page(id)
}

// Flush (some) dirty pages to the map, after clearing their dirty flag.
// @param[in] txn the transaction that's being committed
// @param[in] keep number of initial pages in dirty_list to keep dirty.
// @return 0 on success, non-zero on failure.
func (t *Transaction) flush(keep bool) error {
	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// TODO: Loop over each dirty page and write it to disk.
	return nil
}

func (t *RWTransaction) DeleteBucket(name string) error {
	// TODO: Remove from main DB.
	// TODO: Delete entry from system bucket.
	// TODO: Free all pages.
	// TODO: Remove cursor.

	return nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(count int) (*page, error) {
	// TODO: Find a continuous block of free pages.
	// TODO: If no free pages are available, resize the mmap to allocate more.
	return nil, nil
}

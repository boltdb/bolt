package bolt

import (
	"unsafe"
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction
	bpages map[pgid]*bpage
	lpages map[pgid]*lpage
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
	// Check if bucket already exists.
	if b := t.Bucket(name); b != nil {
		return &Error{"bucket already exists", nil}
	}

	// Create a new bucket entry.
	var buf [unsafe.Sizeof(bucket{})]byte
	var raw = (*bucket)(unsafe.Pointer(&buf[0]))
	raw.root = 0

	// TODO: Delete node first.

	// Insert new node.
	c := t.sys.cursor()
	c.Goto([]byte(name))
	t.lpage(c.page().id).put([]byte(name), buf[:])

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
		return &Error{"bucket not found", nil}
	}

	// Validate the key and data size.
	if len(key) == 0 {
		return &Error{"key required", nil}
	} else if len(key) > MaxKeySize {
		return &Error{"key too large", nil}
	} else if len(value) > MaxDataSize {
		return &Error{"data too large", nil}
	}

	// Insert a new node.
	c := b.cursor()
	c.Goto(key)
	t.lpage(c.page().id).put(key, value)

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
func (t *RWTransaction) allocate(size int) (*page, error) {
	// TODO: Find a continuous block of free pages.
	// TODO: If no free pages are available, resize the mmap to allocate more.
	return nil, nil
}

// lpage returns a deserialized leaf page.
func (t *RWTransaction) lpage(id pgid) *lpage {
	if t.lpages != nil {
		if p := t.lpages[id]; p != nil {
			return p
		}
	}

	// Read raw page and deserialize.
	p := &lpage{}
	p.read(t.page(id))
	t.lpages[id] = p

	return p
}

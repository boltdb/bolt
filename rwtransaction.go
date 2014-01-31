package bolt

import (
	"sort"
	"unsafe"
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction
	branches map[pgid]*branch
	leafs    map[pgid]*leaf
}

// init initializes the transaction.
func (t *RWTransaction) init(db *DB) {
	t.Transaction.init(db)
	t.pages = make(map[pgid]*page)

	// Copy the meta and increase the transaction id. 
	t.meta = &meta{}
	db.meta().copy(t.meta)
	t.meta.txnid += txnid(2)
}

// CreateBucket creates a new bucket.
func (t *RWTransaction) CreateBucket(name string) error {
	// Check if bucket already exists.
	if b := t.Bucket(name); b != nil {
		return &Error{"bucket already exists", nil}
	} else if len(name) == 0 {
		return &Error{"bucket name cannot be blank", nil}
	} else if len(name) > MaxBucketNameSize {
		return &Error{"bucket name too large", nil}
	}

	// Create a blank root leaf page.
	p := t.allocate(1)
	p.flags = p_leaf

	// Add bucket to system page.
	t.sys.put(name, &bucket{root: p.id})

	return nil
}

// DropBucket deletes a bucket.
func (t *RWTransaction) DeleteBucket(name string) error {
	// Remove from system page.
	t.sys.del(name)

	// TODO: Delete entry from system bucket.
	// TODO: Free all pages.
	// TODO: Remove cursor.
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
	c := b.Cursor()
	c.Goto(key)
	t.leaf(c).put(key, value)

	return nil
}

func (t *RWTransaction) Delete(name string, key []byte) error {
	// TODO: Traverse to the correct node.
	// TODO: If missing, exit.
	// TODO: Remove node from page.
	// TODO: If page is empty then add it to the freelist.
	return nil
}

// Commit writes all changes to disk.
func (t *RWTransaction) Commit() error {
	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// TODO: Rebalance.

	// Spill data onto dirty pages.
	t.spill()

	// Spill system page.
	p := t.allocate((t.sys.size() / t.db.pageSize) + 1)
	t.sys.write(p)

	// Write dirty pages to disk.
	if err := t.write(); err != nil {
		return err
	}

	// Update the meta.
	t.meta.sys = p.id

	// Write meta to disk.
	if err := t.writeMeta(); err != nil {
		return err
	}

	return nil
}

func (t *RWTransaction) Rollback() {
	t.close()
}

func (t *RWTransaction) close() {
	// Clear temporary pages.
	t.leafs = nil

	// TODO: Release writer lock.
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(count int) *page {
	// TODO(benbjohnson): Use pages from the freelist.

	// Allocate a set of contiguous pages from the end of the file.
	buf := make([]byte, count*t.db.pageSize)
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.id = t.meta.pgid
	p.overflow = uint32(count - 1)

	// Increment the last page id.
	t.meta.pgid += pgid(count)

	// Save it in our page cache.
	t.pages[p.id] = p

	return p
}

// spill writes all the leafs and branches to dirty pages.
func (t *RWTransaction) spill() {
	// Spill leafs first.
	for _, l := range t.leafs {
		t.spillLeaf(l)
	}

	// Sort branches by highest depth first.
	branches := make(branches, 0, len(t.branches))
	for _, b := range t.branches {
		branches = append(branches, b)
	}
	sort.Sort(branches)

	// Spill branches by deepest first.
	for _, b := range branches {
		t.spillBranch(b)
	}
}

// spillLeaf writes a leaf to one or more dirty pages.
func (t *RWTransaction) spillLeaf(l *leaf) {
	parent := l.parent

	// Split leaf, if necessary.
	leafs := l.split(t.db.pageSize)

	// TODO: If this is a root leaf and we split then add a parent branch.

	// Process each resulting leaf.
	previd := leafs[0].pgid
	for index, l := range leafs {
		// Allocate contiguous space for the leaf.
		p := t.allocate((l.size() / t.db.pageSize) + 1)

		// Write the leaf to the page.
		l.write(p)

		// Insert or replace the node in the parent branch with the new pgid.
		if parent != nil {
			parent.put(previd, p.id, l.items[0].key, (index == 0))
			previd = l.pgid
		}
	}
}

// spillBranch writes a branch to one or more dirty pages.
func (t *RWTransaction) spillBranch(l *branch) {
	warn("[pending] RWTransaction.spillBranch()") // TODO
}

// write writes any dirty pages to disk.
func (t *RWTransaction) write() error {
	// TODO(benbjohnson): If our last page id is greater than the mmap size then lock the DB and resize.

	// Sort pages by id.
	pages := make(pages, 0, len(t.pages))
	for _, p := range t.pages {
		pages = append(pages, p)
	}
	sort.Sort(pages)

	// Write pages to disk in order.
	for _, p := range pages {
		size := (int(p.overflow) + 1) * t.db.pageSize
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:size]
		t.db.file.WriteAt(buf, int64(p.id)*int64(t.db.pageSize))
	}

	return nil
}

// writeMeta writes the meta to the disk.
func (t *RWTransaction) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, t.db.pageSize)
	p := t.db.pageInBuffer(buf, 0)
	t.meta.write(p)

	// Write the meta page to file.
	t.db.metafile.WriteAt(buf, int64(p.id)*int64(t.db.pageSize))

	return nil
}

// leaf retrieves a leaf object based on the current position of a cursor.
func (t *RWTransaction) leaf(c *Cursor) *leaf {
	e := c.stack[len(c.stack)-1]
	id := e.page.id

	// Retrieve leaf if it has already been fetched.
	if l := t.leafs[id]; l != nil {
		return l
	}

	// Otherwise create a leaf and cache it.
	l := &leaf{}
	l.read(t.page(id))
	l.parent = t.branch(c.stack[:len(c.stack)-1])
	t.leafs[id] = l

	return l
}

// branch retrieves a branch object based a cursor stack.
// This should only be called from leaf().
func (t *RWTransaction) branch(stack []elem) *branch {
	if len(stack) == 0 {
		return nil
	}

	// Retrieve branch if it has already been fetched.
	e := &stack[len(stack)-1]
	id := e.page.id
	if b := t.branches[id]; b != nil {
		return b
	}

	// Otherwise create a branch and cache it.
	b := &branch{}
	b.read(t.page(id))
	b.depth = len(stack) - 1
	b.parent = t.branch(stack[:len(stack)-1])
	t.branches[id] = b

	return b
}

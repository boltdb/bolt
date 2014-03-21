package bolt

import (
	"sort"
	"unsafe"
)

// txid represents the internal transaction identifier.
type txid uint64

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
type Tx struct {
	writable bool
	db       *DB
	meta     *meta
	buckets  *buckets
	nodes    map[pgid]*node
	pages    map[pgid]*page
	pending  []*node
}

// init initializes the transaction.
func (t *Tx) init(db *DB) {
	t.db = db
	t.pages = nil

	// Copy the meta page since it can be changed by the writer.
	t.meta = &meta{}
	db.meta().copy(t.meta)

	// Read in the buckets page.
	t.buckets = &buckets{}
	t.buckets.read(t.page(t.meta.buckets))

	if t.writable {
		t.pages = make(map[pgid]*page)
		t.nodes = make(map[pgid]*node)

		// Increment the transaction id.
		t.meta.txid += txid(1)
	}
}

// id returns the transaction id.
func (t *Tx) id() txid {
	return t.meta.txid
}

// DB returns a reference to the database that created the transaction.
func (t *Tx) DB() *DB {
	return t.db
}

// Writable returns whether the transaction can perform write operations.
func (t *Tx) Writable() bool {
	return t.writable
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
func (t *Tx) Bucket(name string) *Bucket {
	b := t.buckets.get(name)
	if b == nil {
		return nil
	}

	return &Bucket{
		bucket: b,
		name:   name,
		tx:     t,
	}
}

// Buckets retrieves a list of all buckets.
func (t *Tx) Buckets() []*Bucket {
	buckets := make([]*Bucket, 0, len(t.buckets.items))
	for name, b := range t.buckets.items {
		bucket := &Bucket{
			bucket: b,
			name:   name,
			tx:     t,
		}
		buckets = append(buckets, bucket)
	}
	sort.Sort(bucketsByName(buckets))
	return buckets
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (t *Tx) CreateBucket(name string) error {
	if !t.writable {
		return ErrTxNotWritable
	} else if b := t.Bucket(name); b != nil {
		return ErrBucketExists
	} else if len(name) == 0 {
		return ErrBucketNameRequired
	} else if len(name) > MaxBucketNameSize {
		return ErrBucketNameTooLarge
	}

	// Create a blank root leaf page.
	p, err := t.allocate(1)
	if err != nil {
		return err
	}
	p.flags = leafPageFlag

	// Add bucket to buckets page.
	t.buckets.put(name, &bucket{root: p.id})

	return nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (t *Tx) CreateBucketIfNotExists(name string) error {
	err := t.CreateBucket(name)
	if err != nil && err != ErrBucketExists {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found.
func (t *Tx) DeleteBucket(name string) error {
	if !t.writable {
		return ErrTxNotWritable
	}

	b := t.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}

	// Remove from buckets page.
	t.buckets.del(name)

	// Free all pages.
	t.forEachPage(b.root, 0, func(p *page, depth int) {
		t.db.freelist.free(t.id(), p)
	})

	return nil
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs.
func (t *Tx) Commit() error {
	if t.db == nil {
		return nil
	} else if !t.writable {
		t.Rollback()
		return nil
	}
	defer t.close()

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// Rebalance and spill data onto dirty pages.
	t.rebalance()
	t.spill()

	// Spill buckets page.
	p, err := t.allocate((t.buckets.size() / t.db.pageSize) + 1)
	if err != nil {
		return err
	}
	t.buckets.write(p)

	// Write dirty pages to disk.
	if err := t.write(); err != nil {
		return err
	}

	// Update the meta.
	t.meta.buckets = p.id

	// Write meta to disk.
	if err := t.writeMeta(); err != nil {
		return err
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates.
func (t *Tx) Rollback() {
	t.close()
}

func (t *Tx) close() {
	if t.db != nil {
		if t.writable {
			t.db.rwlock.Unlock()
		} else {
			t.db.removeTx(t)
		}
		t.db = nil
	}
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *Tx) allocate(count int) (*page, error) {
	p, err := t.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	t.pages[p.id] = p

	return p, nil
}

// rebalance attempts to balance all nodes.
func (t *Tx) rebalance() {
	for _, n := range t.nodes {
		n.rebalance()
	}
}

// spill writes all the nodes to dirty pages.
func (t *Tx) spill() error {
	// Keep track of the current root nodes.
	// We will update this at the end once all nodes are created.
	type root struct {
		node *node
		pgid pgid
	}
	var roots []root

	// Sort nodes by highest depth first.
	nodes := make(nodesByDepth, 0, len(t.nodes))
	for _, n := range t.nodes {
		nodes = append(nodes, n)
	}
	sort.Sort(nodes)

	// Spill nodes by deepest first.
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]

		// Save existing root buckets for later.
		if n.parent == nil && n.pgid != 0 {
			roots = append(roots, root{n, n.pgid})
		}

		// Split nodes into appropriate sized nodes.
		// The first node in this list will be a reference to n to preserve ancestry.
		newNodes := n.split(t.db.pageSize)
		t.pending = newNodes

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{tx: t, isLeaf: false}
			nodes = append(nodes, n.parent)
		}

		// Add node's page to the freelist.
		if n.pgid > 0 {
			t.db.freelist.free(t.id(), t.page(n.pgid))
		}

		// Write nodes to dirty pages.
		for i, newNode := range newNodes {
			// Allocate contiguous space for the node.
			p, err := t.allocate((newNode.size() / t.db.pageSize) + 1)
			if err != nil {
				return err
			}

			// Write the node to the page.
			newNode.write(p)
			newNode.pgid = p.id
			newNode.parent = n.parent

			// The first node should use the existing entry, other nodes are inserts.
			var oldKey []byte
			if i == 0 {
				oldKey = n.key
			} else {
				oldKey = newNode.inodes[0].key
			}

			// Update the parent entry.
			if newNode.parent != nil {
				newNode.parent.put(oldKey, newNode.inodes[0].key, nil, newNode.pgid)
			}
		}

		t.pending = nil
	}

	// Update roots with new roots.
	for _, root := range roots {
		t.buckets.updateRoot(root.pgid, root.node.root().pgid)
	}

	// Clear out nodes now that they are all spilled.
	t.nodes = make(map[pgid]*node)

	return nil
}

// write writes any dirty pages to disk.
func (t *Tx) write() error {
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
		offset := int64(p.id) * int64(t.db.pageSize)
		if _, err := t.db.file.WriteAt(buf, offset); err != nil {
			return err
		}
	}

	// Clear out page cache.
	t.pages = make(map[pgid]*page)

	return nil
}

// writeMeta writes the meta to the disk.
func (t *Tx) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, t.db.pageSize)
	p := t.db.pageInBuffer(buf, 0)
	t.meta.write(p)

	// Write the meta page to file.
	t.db.metafile.WriteAt(buf, int64(p.id)*int64(t.db.pageSize))

	return nil
}

// node creates a node from a page and associates it with a given parent.
func (t *Tx) node(pgid pgid, parent *node) *node {
	// Retrieve node if it's already been created.
	if t.nodes == nil {
		return nil
	} else if n := t.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{tx: t, parent: parent}
	if n.parent != nil {
		n.depth = n.parent.depth + 1
	}
	n.read(t.page(pgid))
	t.nodes[pgid] = n

	return n
}

// dereference removes all references to the old mmap.
func (t *Tx) dereference() {
	for _, n := range t.nodes {
		n.dereference()
	}

	for _, n := range t.pending {
		n.dereference()
	}
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	if t.pages != nil {
		if p, ok := t.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return t.db.page(id)
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (t *Tx) pageNode(id pgid) (*page, *node) {
	if t.nodes != nil {
		if n := t.nodes[id]; n != nil {
			return nil, n
		}
	}
	return t.page(id), nil
}

// forEachPage iterates over every page within a given page and executes a function.
func (t *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := t.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			t.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

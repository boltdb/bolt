package bolt

import (
	"sort"
	"unsafe"
)

// RWTx represents a transaction that can read and write data.
// Only one read/write transaction can be active for a database at a time.
// RWTx is composed of a read-only transaction so it can also use
// functions provided by Tx.
type RWTx struct {
	Tx
	pending []*node
}

// init initializes the transaction.
func (t *RWTx) init(db *DB) {
	t.Tx.init(db)
	t.Tx.rwtx = t
	t.pages = make(map[pgid]*page)
	t.nodes = make(map[pgid]*node)

	// Increment the transaction id.
	t.meta.txid += txid(1)
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (t *RWTx) CreateBucket(name string) error {
	// Check if bucket already exists.
	if b := t.Bucket(name); b != nil {
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
func (t *RWTx) CreateBucketIfNotExists(name string) error {
	err := t.CreateBucket(name)
	if err != nil && err != ErrBucketExists {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found.
func (t *RWTx) DeleteBucket(name string) error {
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
func (t *RWTx) Commit() error {
	if t.db == nil {
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
func (t *RWTx) Rollback() {
	t.close()
}

func (t *RWTx) close() {
	if t.db != nil {
		t.db.rwlock.Unlock()
		t.db = nil
	}
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTx) allocate(count int) (*page, error) {
	p, err := t.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	t.pages[p.id] = p

	return p, nil
}

// rebalance attempts to balance all nodes.
func (t *RWTx) rebalance() {
	for _, n := range t.nodes {
		n.rebalance()
	}
}

// spill writes all the nodes to dirty pages.
func (t *RWTx) spill() error {
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
func (t *RWTx) write() error {
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
func (t *RWTx) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, t.db.pageSize)
	p := t.db.pageInBuffer(buf, 0)
	t.meta.write(p)

	// Write the meta page to file.
	t.db.metafile.WriteAt(buf, int64(p.id)*int64(t.db.pageSize))

	return nil
}

// node creates a node from a page and associates it with a given parent.
func (t *RWTx) node(pgid pgid, parent *node) *node {
	// Retrieve node if it has already been fetched.
	if n := t.Tx.node(pgid); n != nil {
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
func (t *RWTx) dereference() {
	for _, n := range t.nodes {
		n.dereference()
	}

	for _, n := range t.pending {
		n.dereference()
	}
}

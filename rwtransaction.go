package bolt

import (
	"sort"
	"unsafe"
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction
	nodes map[pgid]*node
}

// init initializes the transaction.
func (t *RWTransaction) init(db *DB) {
	t.Transaction.init(db)
	t.pages = make(map[pgid]*page)

	// Copy the meta and increase the transaction id. 
	t.meta = &meta{}
	db.meta().copy(t.meta)
	t.meta.txnid += txnid(1)
}

// CreateBucket creates a new bucket.
func (t *RWTransaction) CreateBucket(name string) error {
	// Check if bucket already exists.
	if b := t.Bucket(name); b != nil {
		return &Error{"bucket already exists", nil}
	} else if len(name) == 0 {
		return &Error{"bucket name cannot be blank", nil}
	} else if len(name) > MaxBucketNameSize {
		return &Error{"bucket name too long", nil}
	}

	// Create a blank root leaf page.
	p := t.allocate(1)
	p.flags = p_leaf

	// Add bucket to buckets page.
	t.buckets.put(name, &bucket{root: p.id})

	return nil
}

// DropBucket deletes a bucket.
func (t *RWTransaction) DeleteBucket(name string) error {
	// Remove from buckets page.
	t.buckets.del(name)

	// TODO(benbjohnson): Free all pages.
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

	// Move cursor to correct position.
	c := b.cursor()
	c.Get(key)

	// Insert the key/value.
	c.node(t).put(key, key, value, 0)

	return nil
}

func (t *RWTransaction) Delete(name string, key []byte) error {
	b := t.Bucket(name)
	if b == nil {
		return &Error{"bucket not found", nil}
	}

	// Move cursor to correct position.
	c := b.cursor()
	c.Get(key)

	// Delete the node if we have a matching key.
	c.node(t).del(key)

	return nil
}

// Commit writes all changes to disk.
func (t *RWTransaction) Commit() error {
	defer t.close()

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// TODO(benbjohnson): Move rebalancing to occur immediately after deletion (?).

	// Rebalance and spill data onto dirty pages.
	t.rebalance()
	t.spill()

	// Spill buckets page.
	p := t.allocate((t.buckets.size() / t.db.pageSize) + 1)
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

func (t *RWTransaction) Rollback() {
	t.close()
}

func (t *RWTransaction) close() {
	t.db.rwlock.Unlock()
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(count int) *page {
	p := t.db.allocate(count)

	// Save to our page cache.
	t.pages[p.id] = p

	return p
}

// rebalance attempts to balance all nodes.
func (t *RWTransaction) rebalance() {
	for _, n := range t.nodes {
		n.rebalance()
	}
}

// spill writes all the nodes to dirty pages.
func (t *RWTransaction) spill() {
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

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{transaction: t, isLeaf: false}
			nodes = append(nodes, n.parent)
		}

		// Add node's page to the freelist.
		if n.pgid > 0 {
			t.db.freelist.free(t.id(), t.page(n.pgid))
		}

		// Write nodes to dirty pages.
		for i, newNode := range newNodes {
			// Allocate contiguous space for the node.
			p := t.allocate((newNode.size() / t.db.pageSize) + 1)

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
	}

	// Update roots with new roots.
	for _, root := range roots {
		t.buckets.updateRoot(root.pgid, root.node.root().pgid)
	}

	// Clear out nodes now that they are all spilled.
	t.nodes = make(map[pgid]*node)
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
		offset := int64(p.id) * int64(t.db.pageSize)
		if _, err := t.db.file.WriteAt(buf, offset); err != nil {
			return err
		}
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

// node creates a node from a page and associates it with a given parent.
func (t *RWTransaction) node(pgid pgid, parent *node) *node {
	// Retrieve node if it has already been fetched.
	if n := t.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{transaction: t, parent: parent}
	if n.parent != nil {
		n.depth = n.parent.depth + 1
	}
	n.read(t.page(pgid))
	t.nodes[pgid] = n

	return n
}

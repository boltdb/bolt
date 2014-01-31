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
	c := b.cursor()
	c.Get(key)
	t.node(c.stack).put(key, key, value, 0)

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
	// Clear nodes.
	t.nodes = nil

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

		// Split nodes and write them.
		newNodes := n.split(t.db.pageSize)

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{
				isLeaf: false,
				key:    newNodes[0].inodes[0].key,
				depth:  n.depth - 1,
				inodes: make(inodes, 0),
			}
			nodes = append(nodes, n.parent)
			sort.Sort(nodes)
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
		for _, b := range t.sys.buckets {
			if b.root == root.pgid {
				b.root = root.node.root().pgid
				break
			}
		}
	}
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

// node retrieves a node based a cursor stack.
func (t *RWTransaction) node(stack []elem) *node {
	if len(stack) == 0 {
		return nil
	}

	// Retrieve branch if it has already been fetched.
	e := &stack[len(stack)-1]
	id := e.page.id
	if n := t.nodes[id]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{}
	n.read(t.page(id))
	n.depth = len(stack) - 1
	n.parent = t.node(stack[:len(stack)-1])
	t.nodes[id] = n

	return n
}

package bolt

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unsafe"
)

var (
	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists is returned when creating a bucket that already exists.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")

	// ErrIncompatibleValue is returned when trying create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	ErrIncompatibleValue = errors.New("incompatible value")

	// ErrSequenceOverflow is returned when the next sequence number will be
	// larger than the maximum integer size.
	ErrSequenceOverflow = errors.New("sequence overflow")
)

// Bucket represents a collection of key/value pairs inside the database.
type Bucket struct {
	*bucket
	tx      *Tx
	buckets map[string]*Bucket
	nodes   map[pgid]*node
	pending []*node
}

// bucket represents the on-file representation of a bucket.
type bucket struct {
	root     pgid
	sequence uint64
}

// newBucket returns a new bucket associated with a transaction.
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx}
	b.buckets = make(map[string]*Bucket)
	if tx.writable {
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	b.tx.stats.CursorCount++

	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
func (b *Bucket) Bucket(name []byte) *Bucket {
	if child := b.buckets[string(name)]; child != nil {
		return child
	}

	// Move cursor to key.
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// Return nil if the key doesn't exist or it is not a bucket.
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// Otherwise create a bucket and cache it.
	var child = newBucket(b.tx)
	child.bucket = &bucket{}
	*child.bucket = *(*bucket)(unsafe.Pointer(&v[0]))
	b.buckets[string(name)] = &child

	return &child
}

// CreateBucket creates a new bucket at the given key.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
func (b *Bucket) CreateBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.tx.writable {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrBucketNameRequired
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key.
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return ErrBucketExists
		} else {
			return ErrIncompatibleValue
		}
	}

	// Create a blank root leaf page.
	p, err := b.tx.allocate(1)
	if err != nil {
		return err
	}
	p.flags = leafPageFlag

	// Insert key/value.
	value := make([]byte, unsafe.Sizeof(bucket{}))
	bucket := (*bucket)(unsafe.Pointer(&value[0]))
	bucket.root = p.id

	// Insert into node.
	c.node().put(key, key, value, 0, bucketLeafFlag)

	return nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (b *Bucket) CreateBucketIfNotExists(key []byte) error {
	err := b.CreateBucket(key)
	if err != nil && err != ErrBucketExists {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if bucket doesn't exist or is not a bucket.
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// Recursively delete all child buckets.
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove cached copy.
	delete(b.buckets, string(key))

	// Release all bucket pages to freelist.
	b.tx.forEachPage(child.root, 0, func(p *page, _ int) {
		b.tx.db.freelist.free(b.tx.id(), p)
	})

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	// Return nil if this is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// Return an error if there is already existing bucket value.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (int, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Make sure next sequence number will not be larger than the maximum
	// integer size of the system.
	if b.bucket.sequence == uint64(maxInt) {
		return 0, ErrSequenceOverflow
	}

	// Increment and return the sequence.
	b.bucket.sequence++

	return int(b.bucket.sequence), nil
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stat returns stats on a bucket.
func (b *Bucket) Stat() *BucketStat {
	s := &BucketStat{}
	b.tx.forEachPage(b.root, 0, func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.LeafPageCount++
			s.KeyCount += int(p.count)
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageCount++
		}

		s.OverflowPageCount += int(p.overflow)

		if depth+1 > s.MaxDepth {
			s.MaxDepth = (depth + 1)
		}
	})
	return s
}

// spill writes all the nodes for this bucket to dirty pages.
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	for name, child := range b.buckets {
		if err := child.spill(); err != nil {
			return err
		}

		// Update the child bucket header in this bucket.
		value := make([]byte, unsafe.Sizeof(bucket{}))
		bucket := (*bucket)(unsafe.Pointer(&value[0]))
		*bucket = *child.bucket

		// Update parent node.
		c := b.Cursor()
		k, _, flags := c.seek([]byte(name))
		_assert(bytes.Equal([]byte(name), k), "misplaced bucket header: %x -> %x", []byte(name), k)
		_assert(flags&bucketLeafFlag != 0, "unexpected bucket header flag: %x", flags)
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there are no nodes to spill.
	if len(b.nodes) == 0 {
		return nil
	}

	// Sort nodes by highest depth first.
	nodes := make(nodesByDepth, 0, len(b.nodes))
	for _, n := range b.nodes {
		nodes = append(nodes, n)
	}
	sort.Sort(nodes)

	// Spill nodes by deepest first.
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]

		// Split nodes into appropriate sized nodes.
		// The first node in this list will be a reference to n to preserve ancestry.
		newNodes := n.split(b.tx.db.pageSize)
		b.pending = newNodes

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{bucket: b, isLeaf: false}
			nodes = append(nodes, n.parent)
		}

		// Add node's page to the freelist.
		if n.pgid > 0 {
			b.tx.db.freelist.free(b.tx.id(), b.tx.page(n.pgid))
		}

		// Write nodes to dirty pages.
		for i, newNode := range newNodes {
			// Allocate contiguous space for the node.
			p, err := b.tx.allocate((newNode.size() / b.tx.db.pageSize) + 1)
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
				newNode.parent.put(oldKey, newNode.inodes[0].key, nil, newNode.pgid, 0)
			}

			// Update the statistics.
			b.tx.stats.Spill++
		}

		b.pending = nil
	}

	// Clear out nodes now that they are all spilled.
	b.nodes = make(map[pgid]*node)

	// Update the root node for this bucket.
	b.root = nodes[len(nodes)-1].pgid

	return nil
}

// rebalance attempts to balance all nodes.
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node creates a node from a page and associates it with a given parent.
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")
	// Retrieve node if it's already been created.
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{bucket: b, parent: parent}
	if n.parent != nil {
		n.depth = n.parent.depth + 1
	}
	n.read(b.tx.page(pgid))
	b.nodes[pgid] = n

	// Update statistics.
	b.tx.stats.NodeCount++

	return n
}

// dereference removes all references to the old mmap.
func (b *Bucket) dereference() {
	for _, n := range b.nodes {
		n.dereference()
	}

	for _, n := range b.pending {
		n.dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}

	// Update statistics
	b.tx.stats.NodeDeref += len(b.nodes) + len(b.pending)
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}
	return b.tx.page(id), nil
}

// BucketStat represents stats on a bucket such as branch pages and leaf pages.
type BucketStat struct {
	BranchPageCount   int
	LeafPageCount     int
	OverflowPageCount int
	KeyCount          int
	MaxDepth          int
}

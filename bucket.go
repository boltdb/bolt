package bolt

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = 4294967295
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

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

// Bucket represents a collection of key/value pairs inside the database.
type Bucket struct {
	*bucket
	tx       *Tx
	buckets  map[string]*Bucket
	rootNode *node
	nodes    map[pgid]*node
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

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root returns the root of the bucket.
func (b *Bucket) Root() pgid {
	return b.root
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

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key.
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		} else {
			return nil, ErrIncompatibleValue
		}
	}

	// Create a blank root leaf page.
	p, err := b.tx.allocate(1)
	if err != nil {
		return nil, err
	}
	p.flags = leafPageFlag

	// Insert key/value.
	value := make([]byte, unsafe.Sizeof(bucket{}))
	bucket := (*bucket)(unsafe.Pointer(&value[0]))
	bucket.root = p.id

	// Insert into node.
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	return b.Bucket(key), nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
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
	key = cloneBytes(key)
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
func (b *Bucket) Stats() BucketStats {
	var s BucketStats
	pageSize := b.tx.db.pageSize
	b.tx.forEachPage(b.root, 0, func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.LeafPageN++
			s.KeyN += int(p.count)
			lastElement := p.leafPageElement(p.count - 1)
			used := pageHeaderSize + (leafPageElementSize * int(p.count-1))
			used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			s.LeafInuse += used
			s.LeafOverflowN += int(p.overflow)
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize
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

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	b.root = b.rootNode.pgid

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

	// Otherwise create a branch node and cache it.
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
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

	for _, child := range b.buckets {
		child.dereference()
	}

	// Update statistics
	b.tx.stats.NodeDeref += len(b.nodes)
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

// BucketStats records statistics about resources used by a bucket.
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}

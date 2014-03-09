package bolt

import (
	"bytes"
)

// Bucket represents a collection of key/value pairs inside the database.
type Bucket struct {
	*bucket
	name string
	tx   *Tx
}

// bucket represents the on-file representation of a bucket.
type bucket struct {
	root     pgid
	sequence uint64
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.name
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (b *Bucket) Cursor() *Cursor {
	return &Cursor{
		tx:    b.tx,
		root:  b.root,
		stack: make([]elemRef, 0),
	}
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist.
func (b *Bucket) Get(key []byte) []byte {
	c := b.Cursor()
	k, v := c.Seek(key)

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
	if !b.Writable() {
		return ErrBucketNotWritable
	}

	// Validate the key and data size.
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	c := b.Cursor()
	c.Seek(key)

	// Insert the key/value.
	c.node(b.tx).put(key, key, value, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	if !b.Writable() {
		return ErrBucketNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	c.Seek(key)

	// Delete the node if we have a matching key.
	c.node(b.tx).del(key)

	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (int, error) {
	if !b.Writable() {
		return 0, ErrBucketNotWritable
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
// An error is returned if the bucket cannot be found.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
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

// BucketStat represents stats on a bucket such as branch pages and leaf pages.
type BucketStat struct {
	BranchPageCount   int
	LeafPageCount     int
	OverflowPageCount int
	KeyCount          int
	MaxDepth          int
}

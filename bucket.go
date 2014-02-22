package bolt

import (
	"bytes"
)

// Bucket represents a collection of key/value pairs inside the database.
// All keys inside the bucket are unique.
//
// Accessing or changing data from a Bucket whose Transaction has closed will cause a panic.
type Bucket struct {
	*bucket
	name        string
	transaction *Transaction
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

// Cursor creates a new cursor for this bucket.
func (b *Bucket) Cursor() *Cursor {
	_assert(b.transaction.isOpen(), "transaction not open")
	return &Cursor{
		transaction: b.transaction,
		root:        b.root,
		stack:       make([]pageElementRef, 0),
	}
}

// Get retrieves the value for a key in a named bucket.
// Returns a nil value if the key does not exist.
func (b *Bucket) Get(key []byte) []byte {
	_assert(b.transaction.isOpen(), "transaction not open")
	c := b.Cursor()
	k, v := c.Seek(key)

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}

	return v
}

// Put sets the value for a key inside of the bucket.
// If the key exist then its previous value will be overwritten.
// Returns an error if bucket was created from a read-only transaction, if the
// key is blank, if the key is too large, or if the value is too large.
func (b *Bucket) Put(key []byte, value []byte) error {
	_assert(b.transaction.isOpen(), "transaction not open")
	if !b.transaction.writable {
		return ErrTransactionNotWritable
	} else if len(key) == 0 {
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
	c.node(b.transaction).put(key, key, value, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	_assert(b.transaction.isOpen(), "transaction not open")
	if !b.transaction.writable {
		return ErrTransactionNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	c.Seek(key)

	// Delete the node if we have a matching key.
	c.node(c.transaction).del(key)

	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
// Returns an error if the bucket was created from a read-only transaction or
// if the next sequence will overflow the int type.
func (b *Bucket) NextSequence() (int, error) {
	_assert(b.transaction.isOpen(), "transaction not open")
	if !b.transaction.writable {
		return 0, ErrTransactionNotWritable
	} else if b.bucket.sequence == uint64(maxInt) {
		return 0, ErrSequenceOverflow
	}

	// Increment and return the sequence.
	b.bucket.sequence++

	return int(b.bucket.sequence), nil
}

// ForEach executes a function for each key/value pair in a bucket.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	_assert(b.transaction.isOpen(), "transaction not open")
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
	_assert(b.transaction.isOpen(), "transaction not open")
	s := &BucketStat{}
	b.transaction.forEachPage(b.root, 0, func(p *page, depth int) {
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

package bolt

import (
	"unsafe"
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a DB at a time.
type RWTransaction struct {
	Transaction
	branches map[pgid]*branch
	leafs map[pgid]*leaf
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

	// Insert new node.
	c := t.sys.cursor()
	c.Goto([]byte(name))
	t.leaf(c.page().id).put([]byte(name), buf[:])

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
	t.leaf(c.page().id).put(key, value)

	return nil
}

func (t *RWTransaction) Delete(key []byte) error {
	// TODO: Traverse to the correct node.
	// TODO: If missing, exit.
	// TODO: Remove node from page.
	// TODO: If page is empty then add it to the freelist.
	return nil
}

func (t *RWTransaction) Commit() error {
	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.


	// TODO: Flush data.

	// TODO: Update meta.
	// TODO: Write meta.

	return nil
}

func (t *RWTransaction) Rollback() error {
	return t.close()
}

func (t *RWTransaction) close() error {
	// Clear temporary pages.
	t.leafs = nil

	// TODO: Release writer lock.

	return nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(size int) (*page, error) {
	// TODO: Find a continuous block of free pages.
	// TODO: If no free pages are available, resize the mmap to allocate more.
	return nil, nil
}

// leaf returns a deserialized leaf page.
func (t *RWTransaction) leaf(id pgid) *leaf {
	if t.leafs != nil {
		if l := t.leafs[id]; l != nil {
			return l
		}
	}

	// Read raw page and deserialize.
	l := &leaf{}
	l.read(t.page(id))
	t.leafs[id] = l

	return l
}

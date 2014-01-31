package bolt

type Bucket struct {
	*bucket
	name        string
	transaction *Transaction
}

type bucket struct {
	root pgid
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.name
}

// cursor creates a new cursor for this bucket.
func (b *Bucket) cursor() *Cursor {
	return &Cursor{
		transaction: b.transaction,
		root:        b.root,
		stack:       make([]elem, 0),
	}
}

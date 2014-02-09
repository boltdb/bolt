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
		stack:       make([]pageElementRef, 0),
	}
}

func (b *Bucket) Stat() *Stat {
	// TODO: Calculate size, depth, page count (by type), entry count, readers, etc.
	return nil
}

type Stat struct {
	PageSize          int
	Depth             int
	BranchPageCount   int
	LeafPageCount     int
	OverflowPageCount int
	EntryCount        int
}

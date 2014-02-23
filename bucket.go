package bolt

// Bucket represents a collection of key/value pairs inside the database.
// All keys inside the bucket are unique. The Bucket type is not typically used
// directly. Instead the bucket name is typically passed into the Get(), Put(),
// or Delete() functions.
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

// cursor creates a new cursor for this bucket.
func (b *Bucket) cursor() *Cursor {
	return &Cursor{
		transaction: b.transaction,
		root:        b.root,
		stack:       make([]pageElementRef, 0),
	}
}

// Stat returns stats on a bucket.
func (b *Bucket) Stat() *BucketStat {
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

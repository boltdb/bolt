package bolt

type bucketid uint32

type Bucket struct {
	*bucket
	name string
	transaction Transaction,
	cursors []*Cursor,
}

type bucket struct {
	id       bucketid
	flags    uint32
	root     pgid
	branches pgid
	leafs    pgid
	entries  uint64
}

func (b *Bucket) Close() error {
	// TODO: Close cursors.
	return nil
}

func (b *Bucket) Cursor() (*Cursor, error) {
	if b.transaction == nil {
		return nil, InvalidBucketError
	}

	c := &Cursor{
		bucket: b,
		stack: make([]elem, 0),
	}

	return nil
}

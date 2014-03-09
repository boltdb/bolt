package bolt

// Tx represents a read-only transaction on the database.
// It can be used for retrieving values for keys as well as creating cursors for
// iterating over the data.
//
// IMPORTANT: You must close transactions when you are done with them. Pages
// can not be reclaimed by the writer until no more transactions are using them.
// A long running read transaction can cause the database to quickly grow.
type Tx struct {
	db      *DB
	rwtx    *RWTx
	meta    *meta
	buckets *buckets
	nodes   map[pgid]*node
	pages   map[pgid]*page
}

// txid represents the internal transaction identifier.
type txid uint64

// init initializes the transaction and associates it with a database.
func (t *Tx) init(db *DB) {
	t.db = db
	t.pages = nil

	// Copy the meta page since it can be changed by the writer.
	t.meta = &meta{}
	db.meta().copy(t.meta)

	// Read in the buckets page.
	t.buckets = &buckets{}
	t.buckets.read(t.page(t.meta.buckets))
}

// id returns the transaction id.
func (t *Tx) id() txid {
	return t.meta.txid
}

// Close closes the transaction and releases any pages it is using.
func (t *Tx) Close() {
	if t.db != nil {
		if t.rwtx != nil {
			t.rwtx.Rollback()
		} else {
			t.db.removeTx(t)
			t.db = nil
		}
	}
}

// DB returns a reference to the database that created the transaction.
func (t *Tx) DB() *DB {
	return t.db
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
func (t *Tx) Bucket(name string) *Bucket {
	b := t.buckets.get(name)
	if b == nil {
		return nil
	}

	return &Bucket{
		bucket: b,
		name:   name,
		tx:     t,
		rwtx:   t.rwtx,
	}
}

// Buckets retrieves a list of all buckets.
func (t *Tx) Buckets() []*Bucket {
	buckets := make([]*Bucket, 0, len(t.buckets.items))
	for name, b := range t.buckets.items {
		bucket := &Bucket{
			bucket: b,
			name:   name,
			tx:     t,
			rwtx:   t.rwtx,
		}
		buckets = append(buckets, bucket)
	}
	return buckets
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	if t.pages != nil {
		if p, ok := t.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return t.db.page(id)
}

// node returns a reference to the in-memory node for a given page, if it exists.
func (t *Tx) node(id pgid) *node {
	if t.nodes == nil {
		return nil
	}
	return t.nodes[id]
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (t *Tx) pageNode(id pgid) (*page, *node) {
	if n := t.node(id); n != nil {
		return nil, n
	}
	return t.page(id), nil
}

// forEachPage iterates over every page within a given page and executes a function.
func (t *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := t.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			t.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

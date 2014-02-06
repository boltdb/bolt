package bolt

const (
	ps_modify   = 1
	ps_rootonly = 2
	ps_first    = 4
	ps_last     = 8
)

type txnid uint64

type Transaction struct {
	id      int
	db      *DB
	meta    *meta
	buckets *buckets
	pages   map[pgid]*page
}

// init initializes the transaction and associates it with a database.
func (t *Transaction) init(db *DB) {
	t.db = db
	t.meta = db.meta()
	t.pages = nil

	t.buckets = &buckets{}
	t.buckets.read(t.page(t.meta.buckets))
}

func (t *Transaction) Close() {
	// TODO: Close buckets.
}

func (t *Transaction) DB() *DB {
	return t.db
}

// Bucket retrieves a bucket by name.
func (t *Transaction) Bucket(name string) *Bucket {
	b := t.buckets.get(name)
	if b == nil {
		return nil
	}

	return &Bucket{
		bucket:      b,
		name:        name,
		transaction: t,
	}
}

// Buckets retrieves a list of all buckets.
func (t *Transaction) Buckets() []*Bucket {
	warn("[pending] Transaction.Buckets()") // TODO
	return nil
}

// Cursor creates a cursor associated with a given bucket.
func (t *Transaction) Cursor(name string) *Cursor {
	b := t.Bucket(name)
	if b == nil {
		return nil
	}
	return b.cursor()
}

// Get retrieves the value for a key in a named bucket.
func (t *Transaction) Get(name string, key []byte) []byte {
	c := t.Cursor(name)
	if c == nil {
		return nil
	}
	return c.Get(key)
}

// stat returns information about a bucket's internal structure.
func (t *Transaction) stat(name string) *Stat {
	// TODO
	return nil
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Transaction) page(id pgid) *page {
	// Check the dirty pages first.
	if t.pages != nil {
		if p, ok := t.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return t.db.page(id)
}

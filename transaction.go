package bolt

import (
	"strings"
	"unsafe"
)

var (
	InvalidTransactionError  = &Error{"txn is invalid", nil}
	BucketAlreadyExistsError = &Error{"bucket already exists", nil}
)

const (
	ps_modify   = 1
	ps_rootonly = 2
	ps_first    = 4
	ps_last     = 8
)

type txnid uint64

type Transaction struct {
	id         int
	db         *DB
	meta       *meta
	sys Bucket
	buckets    map[string]*Bucket
}

// init initializes the transaction and associates it with a database.
func (t *Transaction) init(db *DB, meta *meta) {
	t.db = db
	t.meta = meta
	t.buckets = make(map[string]*Bucket)
	t.sys.transaction = t
	t.sys.bucket = &t.meta.sys
}

func (t *Transaction) Close() error {
	// TODO: Close buckets.
	return nil
}

func (t *Transaction) DB() *DB {
	return t.db
}

// Bucket retrieves a bucket by name.
func (t *Transaction) Bucket(name string) (*Bucket, error) {
	return t.bucket(name)
}

func (t *Transaction) bucket(name string) (*Bucket, error) {
	// Return bucket if it's already been looked up.
	if b := t.buckets[name]; b != nil {
		return b, nil
	}

	// Retrieve bucket data from the system bucket.
	data, err := c.get(&t.sys, []byte(name))
	if err != nil {
		return nil, err
	} else if data == nil {
		return nil, &Error{"bucket not found", nil}
	}

	// Create a bucket that overlays the data.
	b := &Bucket{
		bucket: (*bucket)(unsafe.Pointer(&data[0])),
		name: name,
		transaction: t,
	}
	t.buckets[name] = b

	return b, nil
}

// Cursor creates a cursor associated with a given bucket.
func (t *Transaction) Cursor(b *Bucket) (*Cursor, error) {
	if b == nil {
		return nil, &Error{"bucket required", nil}
	} else 

	// Create a new cursor and associate it with the transaction and bucket.
	c := &Cursor{
		transaction: t,
		bucket:      b,
	}

	// Set the first page if available.
	if b.root != p_invalid {
		p := t.db.page(t.db.data, int(b.root))
		c.top = 0
		c.pages = append(c.pages, p)
	}

	return nil, nil
}

// Get retrieves the value for a key in a given bucket.
func (t *Transaction) Get(name string, key []byte) ([]byte, error) {
	c, err := t.Cursor(name)
	if err != nil {
		return nil, err
	}
	return c.Get(key)
}

func (t *Transaction) page(id int) (*page, error) {
	return t.db.page(id)
}

// Stat returns information about a bucket's internal structure.
func (t *Transaction) Stat(name string) *stat {
	// TODO
	return nil
}

//                                                                            //
//                                                                            //
//                                                                            //
//                                                                            //
//                                                                            //
// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ CONVERTED ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ //
//                                                                            //
//                                                                            //
//                                                                            //
//                                                                            //
//                                                                            //

// Return the data associated with a given node.
// @param[in] txn The transaction for this operation.
// @param[in] leaf The node being read.
// @param[out] data Updated to point to the node's data.
// @return 0 on success, non-zero on failure.
func (t *Transaction) readNode(leaf *node, data []byte) error {
	/*
		MDB_page	*omp;		// overflow page
		pgno_t		 pgno;
		int rc;

		if (!F_ISSET(leaf->mn_flags, F_BIGDATA)) {
			data->mv_size = NODEDSZ(leaf);
			data->mv_data = NODEDATA(leaf);
			return MDB_SUCCESS;
		}

		// Read overflow data.
		data->mv_size = NODEDSZ(leaf);
		memcpy(&pgno, NODEDATA(leaf), sizeof(pgno));
		if ((rc = mdb_page_get(txn, pgno, &omp, NULL)) != 0) {
			DPRINTF(("read overflow page %"Z"u failed", pgno));
			return rc;
		}
		data->mv_data = METADATA(omp);

		return MDB_SUCCESS;
	*/
	return nil
}

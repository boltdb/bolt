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
	dirty      bool
	spilled    bool
	err        error
	meta       *meta
	sysfree    Bucket
	sysbuckets Bucket
	buckets    map[string]*Bucket
	cursors    map[uint32]*Cursor

	pgno       int
	freePages  []pgno
	spillPages []pgno
	dirtyList  []pgno
	reader     *reader
	// Implicit from slices? TODO: MDB_dbi mt_numdbs;
	dirty_room int
}

// init initializes the transaction and associates it with a database.
func (t *Transaction) init(db *DB, meta *meta) error {

}

func (t *Transaction) Close() error {
	// TODO: Close cursors.
	// TODO: Close buckets.
	return nil
}

func (t *Transaction) DB() *DB {
	return t.db
}

// Bucket retrieves a bucket by name.
func (t *Transaction) Bucket(name string) (*Bucket, error) {
	if strings.HasPrefix(name, "sys*") {
		return nil, &Error{"system buckets are not available", nil}
	}

	return t.bucket(name)
}

func (t *Transaction) bucket(name string) (*Bucket, error) {
	// TODO: if ((flags & VALID_FLAGS) != flags) return EINVAL;
	// TODO: if (txn->mt_flags & MDB_TXN_ERROR) return MDB_BAD_TXN;

	// Return bucket if it's already been found.
	if b := t.buckets[name]; b != nil {
		return b, nil
	}

	// Open a cursor for the system bucket.
	c, err := t.Cursor(&t.sysbuckets)
	if err != nil {
		return nil, err
	}

	// Retrieve bucket data.
	data, err := c.Get([]byte(name))
	if err != nil {
		return nil, err
	} else if data == nil {
		return nil, &Error{"bucket not found", nil}
	}

	// TODO: Verify.
	// MDB_node *node = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
	// if (!(node->mn_flags & F_SUBDATA))
	//   return MDB_INCOMPATIBLE;

	return nil, nil
}

// Cursor creates a cursor associated with a given bucket.
func (t *Transaction) Cursor(b *Bucket) (*Cursor, error) {
	if b == nil {
		return nil, &Error{"bucket required", nil}
	} else if t.db == nil {
		return nil, InvalidTransactionError
	}

	// TODO: if !(txn->mt_dbflags[dbi] & DB_VALID) return InvalidBucketError
	// TODO: if (txn->mt_flags & MDB_TXN_ERROR) return BadTransactionError

	// Return existing cursor for the bucket if one exists.
	if c := t.cursors[b.id]; c != nil {
		return c, nil
	}

	// Create a new cursor and associate it with the transaction and bucket.
	c := &Cursor{
		transaction: t,
		bucket:      b,
		top:         -1,
		pages:       []*page{},
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

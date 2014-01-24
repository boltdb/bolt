package bolt

import (
	"strings"
	"unsafe"
)

var (
	InvalidTransactionError = &Error{"txn is invalid", nil}
	BucketAlreadyExistsError = &Error{"bucket already exists", nil}
)

const (
	ps_modify   = 1
	ps_rootonly = 2
	ps_first    = 4
	ps_last     = 8
)

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


// Save the freelist as of this transaction to the freeDB.
// This changes the freelist. Keep trying until it stabilizes.
func (t *Transaction) saveFreelist() error {
	/*
			// env->me_pghead[] can grow and shrink during this call.
			// env->me_pglast and txn->mt_free_pgs[] can only grow.
			// Page numbers cannot disappear from txn->mt_free_pgs[].
			MDB_cursor mc;
			MDB_env	*env = txn->mt_env;
			int rc, maxfree_1pg = env->me_maxfree_1pg, more = 1;
			txnid_t	pglast = 0, head_id = 0;
			pgno_t	freecnt = 0, *free_pgs, *mop;
			ssize_t	head_room = 0, total_room = 0, mop_len, clean_limit;

			mdb_cursor_init(&mc, txn, FREE_DBI, NULL);

			if (env->me_pghead) {
				// Make sure first page of freeDB is touched and on freelist 
				rc = mdb_page_search(&mc, NULL, MDB_PS_FIRST|MDB_PS_MODIFY);
				if (rc && rc != MDB_NOTFOUND)
					return rc;
			}

			// MDB_RESERVE cancels meminit in ovpage malloc (when no WRITEMAP)
			clean_limit = (env->me_flags & (MDB_NOMEMINIT|MDB_WRITEMAP))
				? SSIZE_MAX : maxfree_1pg;

			for (;;) {
				// Come back here after each Put() in case freelist changed
				MDB_val key, data;
				pgno_t *pgs;
				ssize_t j;

				// If using records from freeDB which we have not yet
				// deleted, delete them and any we reserved for me_pghead.
				while (pglast < env->me_pglast) {
					rc = mdb_cursor_first(&mc, &key, NULL);
					if (rc)
						return rc;
					pglast = head_id = *(txnid_t *)key.mv_data;
					total_room = head_room = 0;
					mdb_tassert(txn, pglast <= env->me_pglast);
					rc = mdb_cursor_del(&mc, 0);
					if (rc)
						return rc;
				}

				// Save the IDL of pages freed by this txn, to a single record
				if (freecnt < txn->mt_free_pgs[0]) {
					if (!freecnt) {
						// Make sure last page of freeDB is touched and on freelist
						rc = mdb_page_search(&mc, NULL, MDB_PS_LAST|MDB_PS_MODIFY);
						if (rc && rc != MDB_NOTFOUND)
							return rc;
					}
					free_pgs = txn->mt_free_pgs;
					// Write to last page of freeDB
					key.mv_size = sizeof(txn->mt_txnid);
					key.mv_data = &txn->mt_txnid;
					do {
						freecnt = free_pgs[0];
						data.mv_size = MDB_IDL_SIZEOF(free_pgs);
						rc = mdb_cursor_put(&mc, &key, &data, MDB_RESERVE);
						if (rc)
							return rc;
						// Retry if mt_free_pgs[] grew during the Put()
						free_pgs = txn->mt_free_pgs;
					} while (freecnt < free_pgs[0]);
					mdb_midl_sort(free_pgs);
					memcpy(data.mv_data, free_pgs, data.mv_size);
		#if (MDB_DEBUG) > 1
					{
						unsigned int i = free_pgs[0];
						DPRINTF(("IDL write txn %"Z"u root %"Z"u num %u",
							txn->mt_txnid, txn->mt_dbs[FREE_DBI].md_root, i));
						for (; i; i--)
							DPRINTF(("IDL %"Z"u", free_pgs[i]));
					}
		#endif
					continue;
				}

				mop = env->me_pghead;
				mop_len = mop ? mop[0] : 0;

				// Reserve records for me_pghead[]. Split it if multi-page,
				// to avoid searching freeDB for a page range. Use keys in
				// range [1,me_pglast]: Smaller than txnid of oldest reader.
				if (total_room >= mop_len) {
					if (total_room == mop_len || --more < 0)
						break;
				} else if (head_room >= maxfree_1pg && head_id > 1) {
					// Keep current record (overflow page), add a new one
					head_id--;
					head_room = 0;
				}
				// (Re)write {key = head_id, IDL length = head_room}
				total_room -= head_room;
				head_room = mop_len - total_room;
				if (head_room > maxfree_1pg && head_id > 1) {
					// Overflow multi-page for part of me_pghead
					head_room /= head_id; // amortize page sizes
					head_room += maxfree_1pg - head_room % (maxfree_1pg + 1);
				} else if (head_room < 0) {
					// Rare case, not bothering to delete this record
					head_room = 0;
				}
				key.mv_size = sizeof(head_id);
				key.mv_data = &head_id;
				data.mv_size = (head_room + 1) * sizeof(pgno_t);
				rc = mdb_cursor_put(&mc, &key, &data, MDB_RESERVE);
				if (rc)
					return rc;
				// IDL is initially empty, zero out at least the length 
				pgs = (pgno_t *)data.mv_data;
				j = head_room > clean_limit ? head_room : 0;
				do {
					pgs[j] = 0;
				} while (--j >= 0);
				total_room += head_room;
			}

			// Fill in the reserved me_pghead records
			rc = MDB_SUCCESS;
			if (mop_len) {
				MDB_val key, data;

				mop += mop_len;
				rc = mdb_cursor_first(&mc, &key, &data);
				for (; !rc; rc = mdb_cursor_next(&mc, &key, &data, MDB_NEXT)) {
					unsigned flags = MDB_CURRENT;
					txnid_t id = *(txnid_t *)key.mv_data;
					ssize_t	len = (ssize_t)(data.mv_size / sizeof(MDB_ID)) - 1;
					MDB_ID save;

					mdb_tassert(txn, len >= 0 && id <= env->me_pglast);
					key.mv_data = &id;
					if (len > mop_len) {
						len = mop_len;
						data.mv_size = (len + 1) * sizeof(MDB_ID);
						flags = 0;
					}
					data.mv_data = mop -= len;
					save = mop[0];
					mop[0] = len;
					rc = mdb_cursor_put(&mc, &key, &data, flags);
					mop[0] = save;
					if (rc || !(mop_len -= len))
						break;
				}
			}
			return rc;
	*/
	return nil
}




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

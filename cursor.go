package bolt

// TODO: #define CURSOR_STACK		 32

// TODO: #define C_INITIALIZED	0x01	/**< cursor has been initialized and is valid */
// TODO: #define C_EOF	0x02			/**< No more data */
// TODO: #define C_SUB	0x04			/**< Cursor is a sub-cursor */
// TODO: #define C_DEL	0x08			/**< last op was a cursor_del */
// TODO: #define C_SPLITTING	0x20		/**< Cursor is in page_split */
// TODO: #define C_UNTRACK	0x40		/**< Un-track cursor when closing */

// TODO: #define MDB_NOSPILL	0x8000 /** Do not spill pages to disk if txn is getting full, may fail instead */


type Cursor interface {
	First() error
	FirstDup() error
	Get() ([]byte, []byte, error)
	GetRange() ([]byte, []byte, error)
	Current() ([]byte, []byte, error)
	Last()
	LastDup()
	Next() ([]byte, []byte, error)
	NextDup() ([]byte, []byte, error)
	NextNoDup() ([]byte, []byte, error)
	Prev() ([]byte, []byte, error)
	PrevDup() ([]byte, []byte, error)
	PrevNoDup() ([]byte, []byte, error)
	Set() ([]byte, []byte, error)
	SetRange() ([]byte, []byte, error)
}

type cursor struct {
	flags       int
	next        *cursor
	backup      *cursor
	xcursor     *xcursor
	transaction *transaction
	bucketId    int
	bucket      *bucket
	bucketx     *bucketx
	bucketFlag  int
	snum        int
	top         int
	page        []*page
	ki          []int /**< stack of page indices */
}

type xcursor struct {
	cursor     cursor
	bucket     *bucket
	bucketx    *bucketx
	bucketFlag int
}

// Set or clear P_KEEP in dirty, non-overflow, non-sub pages watched by txn.
// @param[in] mc A cursor handle for the current operation.
// @param[in] pflags Flags of the pages to update:
// P_DIRTY to set P_KEEP, P_DIRTY|P_KEEP to clear it.
// @param[in] all No shortcuts. Needed except after a full #mdb_page_flush().
// @return 0 on success, non-zero on failure.
func (c *cursor) xkeep(unsigned pflags, int all) int {
	/*
	enum { Mask = P_SUBP|P_DIRTY|P_KEEP };
	MDB_txn *txn = mc->mc_txn;
	MDB_cursor *m3;
	MDB_xcursor *mx;
	MDB_page *dp, *mp;
	MDB_node *leaf;
	unsigned i, j;
	int rc = MDB_SUCCESS, level;

	// Mark pages seen by cursors
	if (mc->mc_flags & C_UNTRACK)
		mc = NULL;				// will find mc in mt_cursors
	for (i = txn->mt_numdbs;; mc = txn->mt_cursors[--i]) {
		for (; mc; mc=mc->mc_next) {
			if (!(mc->mc_flags & C_INITIALIZED))
				continue;
			for (m3 = mc;; m3 = &mx->mx_cursor) {
				mp = NULL;
				for (j=0; j<m3->mc_snum; j++) {
					mp = m3->mc_pg[j];
					if ((mp->mp_flags & Mask) == pflags)
						mp->mp_flags ^= P_KEEP;
				}
				mx = m3->mc_xcursor;
				// Proceed to mx if it is at a sub-database
				if (! (mx && (mx->mx_cursor.mc_flags & C_INITIALIZED)))
					break;
				if (! (mp && (mp->mp_flags & P_LEAF)))
					break;
				leaf = NODEPTR(mp, m3->mc_ki[j-1]);
				if (!(leaf->mn_flags & F_SUBDATA))
					break;
			}
		}
		if (i == 0)
			break;
	}

	if (all) {
		// Mark dirty root pages
		for (i=0; i<txn->mt_numdbs; i++) {
			if (txn->mt_dbflags[i] & DB_DIRTY) {
				pgno_t pgno = txn->mt_dbs[i].md_root;
				if (pgno == P_INVALID)
					continue;
				if ((rc = mdb_page_get(txn, pgno, &dp, &level)) != MDB_SUCCESS)
					break;
				if ((dp->mp_flags & Mask) == pflags && level <= 1)
					dp->mp_flags ^= P_KEEP;
			}
		}
	}

	return rc;
	*/
	return 0
}

//	Spill pages from the dirty list back to disk.
// This is intended to prevent running into #MDB_TXN_FULL situations,
// but note that they may still occur in a few cases:
//	1) our estimate of the txn size could be too small. Currently this
//	 seems unlikely, except with a large number of #MDB_MULTIPLE items.
//	2) child txns may run out of space if their parents dirtied a
//	 lot of pages and never spilled them. TODO: we probably should do
//	 a preemptive spill during #mdb_txn_begin() of a child txn, if
//	 the parent's dirty_room is below a given threshold.
//
// Otherwise, if not using nested txns, it is expected that apps will
// not run into #MDB_TXN_FULL any more. The pages are flushed to disk
// the same way as for a txn commit, e.g. their P_DIRTY flag is cleared.
// If the txn never references them again, they can be left alone.
// If the txn only reads them, they can be used without any fuss.
// If the txn writes them again, they can be dirtied immediately without
// going thru all of the work of #mdb_page_touch(). Such references are
// handled by #mdb_page_unspill().
//
// Also note, we never spill DB root pages, nor pages of active cursors,
// because we'll need these back again soon anyway. And in nested txns,
// we can't spill a page in a child txn if it was already spilled in a
// parent txn. That would alter the parent txns' data even though
// the child hasn't committed yet, and we'd have no way to undo it if
// the child aborted.
//
// @param[in] m0 cursor A cursor handle identifying the transaction and
//	database for which we are checking space.
// @param[in] key For a put operation, the key being stored.
// @param[in] data For a put operation, the data being stored.
// @return 0 on success, non-zero on failure.
func (c *cursor) spill(MDB_val *key, MDB_val *data) int {
	/*
	MDB_txn *txn = m0->mc_txn;
	MDB_page *dp;
	MDB_ID2L dl = txn->mt_u.dirty_list;
	unsigned int i, j, need;
	int rc;

	if (m0->mc_flags & C_SUB)
		return MDB_SUCCESS;

	// Estimate how much space this op will take
	i = m0->mc_db->md_depth;
	// Named DBs also dirty the main DB
	if (m0->mc_dbi > MAIN_DBI)
		i += txn->mt_dbs[MAIN_DBI].md_depth;
	// For puts, roughly factor in the key+data size
	if (key)
		i += (LEAFSIZE(key, data) + txn->mt_env->me_psize) / txn->mt_env->me_psize;
	i += i;	// double it for good measure
	need = i;

	if (txn->mt_dirty_room > i)
		return MDB_SUCCESS;

	if (!txn->mt_spill_pgs) {
		txn->mt_spill_pgs = mdb_midl_alloc(MDB_IDL_UM_MAX);
		if (!txn->mt_spill_pgs)
			return ENOMEM;
	} else {
		// purge deleted slots
		MDB_IDL sl = txn->mt_spill_pgs;
		unsigned int num = sl[0];
		j=0;
		for (i=1; i<=num; i++) {
			if (!(sl[i] & 1))
				sl[++j] = sl[i];
		}
		sl[0] = j;
	}

	// Preserve pages which may soon be dirtied again
	if ((rc = mdb_pages_xkeep(m0, P_DIRTY, 1)) != MDB_SUCCESS)
		goto done;

	// Less aggressive spill - we originally spilled the entire dirty list,
	// with a few exceptions for cursor pages and DB root pages. But this
	// turns out to be a lot of wasted effort because in a large txn many
	// of those pages will need to be used again. So now we spill only 1/8th
	// of the dirty pages. Testing revealed this to be a good tradeoff,
	// better than 1/2, 1/4, or 1/10.
	if (need < MDB_IDL_UM_MAX / 8)
		need = MDB_IDL_UM_MAX / 8;

	// Save the page IDs of all the pages we're flushing
	// flush from the tail forward, this saves a lot of shifting later on.
	for (i=dl[0].mid; i && need; i--) {
		MDB_ID pn = dl[i].mid << 1;
		dp = dl[i].mptr;
		if (dp->mp_flags & P_KEEP)
			continue;
		// Can't spill twice, make sure it's not already in a parent's
		// spill list.
		if (txn->mt_parent) {
			MDB_txn *tx2;
			for (tx2 = txn->mt_parent; tx2; tx2 = tx2->mt_parent) {
				if (tx2->mt_spill_pgs) {
					j = mdb_midl_search(tx2->mt_spill_pgs, pn);
					if (j <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[j] == pn) {
						dp->mp_flags |= P_KEEP;
						break;
					}
				}
			}
			if (tx2)
				continue;
		}
		if ((rc = mdb_midl_append(&txn->mt_spill_pgs, pn)))
			goto done;
		need--;
	}
	mdb_midl_sort(txn->mt_spill_pgs);

	// Flush the spilled part of dirty list
	if ((rc = mdb_page_flush(txn, i)) != MDB_SUCCESS)
		goto done;

	// Reset any dirty pages we kept that page_flush didn't see
	rc = mdb_pages_xkeep(m0, P_DIRTY|P_KEEP, i);

done:
	txn->mt_flags |= rc ? MDB_TXN_ERROR : MDB_TXN_SPILLS;
	return rc;
	/*
	return 0
}

// Allocate page numbers and memory for writing.  Maintain me_pglast,
// me_pghead and mt_next_pgno.
//
// If there are free pages available from older transactions, they
// are re-used first. Otherwise allocate a new page at mt_next_pgno.
// Do not modify the freedB, just merge freeDB records into me_pghead[]
// and move me_pglast to say which records were consumed.  Only this
// function can create me_pghead and move me_pglast/mt_next_pgno.
// @param[in] mc cursor A cursor handle identifying the transaction and
//	database for which we are allocating.
// @param[in] num the number of pages to allocate.
// @param[out] mp Address of the allocated page(s). Requests for multiple pages
//  will always be satisfied by a single contiguous chunk of memory.
// @return 0 on success, non-zero on failure.
func (c *cursor) allocPage(int num, MDB_page **mp) {
	int rc, retry = INT_MAX;
	MDB_txn *txn = mc->mc_txn;
	MDB_env *env = txn->mt_env;
	pgno_t pgno, *mop = env->me_pghead;
	unsigned i, j, k, mop_len = mop ? mop[0] : 0, n2 = num-1;
	MDB_page *np;
	txnid_t oldest = 0, last;
	MDB_cursor_op op;
	MDB_cursor m2;

	*mp = NULL;

	// If our dirty list is already full, we can't do anything
	if (txn->mt_dirty_room == 0) {
		rc = MDB_TXN_FULL;
		goto fail;
	}

	for (op = MDB_FIRST;; op = MDB_NEXT) {
		MDB_val key, data;
		MDB_node *leaf;
		pgno_t *idl, old_id, new_id;

		// Seek a big enough contiguous page range. Prefer
		// pages at the tail, just truncating the list.
		if (mop_len > n2) {
			i = mop_len;
			do {
				pgno = mop[i];
				if (mop[i-n2] == pgno+n2)
					goto search_done;
			} while (--i > n2);
			if (Max_retries < INT_MAX && --retry < 0)
				break;
		}

		if (op == MDB_FIRST) {	// 1st iteration
			// Prepare to fetch more and coalesce
			oldest = mdb_find_oldest(txn);
			last = env->me_pglast;
			mdb_cursor_init(&m2, txn, FREE_DBI, NULL);
			if (last) {
				op = MDB_SET_RANGE;
				key.mv_data = &last; // will look up last+1
				key.mv_size = sizeof(last);
			}
		}

		last++;
		// Do not fetch more if the record will be too recent
		if (oldest <= last)
			break;
		rc = mdb_cursor_get(&m2, &key, NULL, op);
		if (rc) {
			if (rc == MDB_NOTFOUND)
				break;
			goto fail;
		}
		last = *(txnid_t*)key.mv_data;
		if (oldest <= last)
			break;
		np = m2.mc_pg[m2.mc_top];
		leaf = NODEPTR(np, m2.mc_ki[m2.mc_top]);
		if ((rc = mdb_node_read(txn, leaf, &data)) != MDB_SUCCESS)
			return rc;

		idl = (MDB_ID *) data.mv_data;
		i = idl[0];
		if (!mop) {
			if (!(env->me_pghead = mop = mdb_midl_alloc(i))) {
				rc = ENOMEM;
				goto fail;
			}
		} else {
			if ((rc = mdb_midl_need(&env->me_pghead, i)) != 0)
				goto fail;
			mop = env->me_pghead;
		}
		env->me_pglast = last;
#if (MDB_DEBUG) > 1
		DPRINTF(("IDL read txn %"Z"u root %"Z"u num %u",
			last, txn->mt_dbs[FREE_DBI].md_root, i));
		for (k = i; k; k--)
			DPRINTF(("IDL %"Z"u", idl[k]));
#endif
		// Merge in descending sorted order
		j = mop_len;
		k = mop_len += i;
		mop[0] = (pgno_t)-1;
		old_id = mop[j];
		while (i) {
			new_id = idl[i--];
			for (; old_id < new_id; old_id = mop[--j])
				mop[k--] = old_id;
			mop[k--] = new_id;
		}
		mop[0] = mop_len;
	}

	// Use new pages from the map when nothing suitable in the freeDB
	i = 0;
	pgno = txn->mt_next_pgno;
	if (pgno + num >= env->me_maxpg) {
			DPUTS("DB size maxed out");
			rc = MDB_MAP_FULL;
			goto fail;
	}

search_done:
	if (env->me_flags & MDB_WRITEMAP) {
		np = (MDB_page *)(env->me_map + env->me_psize * pgno);
	} else {
		if (!(np = mdb_page_malloc(txn, num))) {
			rc = ENOMEM;
			goto fail;
		}
	}
	if (i) {
		mop[0] = mop_len -= num;
		// Move any stragglers down 
		for (j = i-num; j < mop_len; )
			mop[++j] = mop[++i];
	} else {
		txn->mt_next_pgno = pgno + num;
	}
	np->mp_pgno = pgno;
	mdb_page_dirty(txn, np);
	*mp = np;

	return MDB_SUCCESS;

fail:
	txn->mt_flags |= MDB_TXN_ERROR;
	return rc;
	*/
	return 0
}

// Copy the used portions of a non-overflow page.
// @param[in] dst page to copy into
// @param[in] src page to copy from
// @param[in] psize size of a page
func (p *page) copyTo(dst *page, size int) {
	/*
	enum { Align = sizeof(pgno_t) };
	indx_t upper = src->mp_upper, lower = src->mp_lower, unused = upper-lower;

	// If page isn't full, just copy the used portion. Adjust
	// alignment so memcpy may copy words instead of bytes.
	if ((unused &= -Align) && !IS_LEAF2(src)) {
		upper &= -Align;
		memcpy(dst, src, (lower + (Align-1)) & -Align);
		memcpy((pgno_t *)((char *)dst+upper), (pgno_t *)((char *)src+upper),
			psize - upper);
	} else {
		memcpy(dst, src, psize - unused);
	}
	*/
}

// Touch a page: make it dirty and re-insert into tree with updated pgno.
// @param[in] mc cursor pointing to the page to be touched
// @return 0 on success, non-zero on failure.
func (c *cursor) page_touch() int {
	/*
	MDB_page *mp = mc->mc_pg[mc->mc_top], *np;
	MDB_txn *txn = mc->mc_txn;
	MDB_cursor *m2, *m3;
	pgno_t	pgno;
	int rc;

	if (!F_ISSET(mp->mp_flags, P_DIRTY)) {
		if (txn->mt_flags & MDB_TXN_SPILLS) {
			np = NULL;
			rc = mdb_page_unspill(txn, mp, &np);
			if (rc)
				goto fail;
			if (np)
				goto done;
		}
		if ((rc = mdb_midl_need(&txn->mt_free_pgs, 1)) ||
			(rc = mdb_page_alloc(mc, 1, &np)))
			goto fail;
		pgno = np->mp_pgno;
		DPRINTF(("touched db %d page %"Z"u -> %"Z"u", DDBI(mc),
			mp->mp_pgno, pgno));
		mdb_cassert(mc, mp->mp_pgno != pgno);
		mdb_midl_xappend(txn->mt_free_pgs, mp->mp_pgno);
		// Update the parent page, if any, to point to the new page
		if (mc->mc_top) {
			MDB_page *parent = mc->mc_pg[mc->mc_top-1];
			MDB_node *node = NODEPTR(parent, mc->mc_ki[mc->mc_top-1]);
			SETPGNO(node, pgno);
		} else {
			mc->mc_db->md_root = pgno;
		}
	} else if (txn->mt_parent && !IS_SUBP(mp)) {
		MDB_ID2 mid, *dl = txn->mt_u.dirty_list;
		pgno = mp->mp_pgno;
		// If txn has a parent, make sure the page is in our
		// dirty list.
		if (dl[0].mid) {
			unsigned x = mdb_mid2l_search(dl, pgno);
			if (x <= dl[0].mid && dl[x].mid == pgno) {
				if (mp != dl[x].mptr) { // bad cursor?
					mc->mc_flags &= ~(C_INITIALIZED|C_EOF);
					txn->mt_flags |= MDB_TXN_ERROR;
					return MDB_CORRUPTED;
				}
				return 0;
			}
		}
		mdb_cassert(mc, dl[0].mid < MDB_IDL_UM_MAX);
		// No - copy it
		np = mdb_page_malloc(txn, 1);
		if (!np)
			return ENOMEM;
		mid.mid = pgno;
		mid.mptr = np;
		rc = mdb_mid2l_insert(dl, &mid);
		mdb_cassert(mc, rc == 0);
	} else {
		return 0;
	}

	mdb_page_copy(np, mp, txn->mt_env->me_psize);
	np->mp_pgno = pgno;
	np->mp_flags |= P_DIRTY;

done:
	// Adjust cursors pointing to mp
	mc->mc_pg[mc->mc_top] = np;
	m2 = txn->mt_cursors[mc->mc_dbi];
	if (mc->mc_flags & C_SUB) {
		for (; m2; m2=m2->mc_next) {
			m3 = &m2->mc_xcursor->mx_cursor;
			if (m3->mc_snum < mc->mc_snum) continue;
			if (m3->mc_pg[mc->mc_top] == mp)
				m3->mc_pg[mc->mc_top] = np;
		}
	} else {
		for (; m2; m2=m2->mc_next) {
			if (m2->mc_snum < mc->mc_snum) continue;
			if (m2->mc_pg[mc->mc_top] == mp) {
				m2->mc_pg[mc->mc_top] = np;
				if ((mc->mc_db->md_flags & MDB_DUPSORT) &&
					m2->mc_ki[mc->mc_top] == mc->mc_ki[mc->mc_top])
				{
					MDB_node *leaf = NODEPTR(np, mc->mc_ki[mc->mc_top]);
					if (!(leaf->mn_flags & F_SUBDATA))
						m2->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
				}
			}
		}
	}
	return 0;

fail:
	txn->mt_flags |= MDB_TXN_ERROR;
	return rc;
	*/

	return 0
}

// Search for key within a page, using binary search.
// Returns the smallest entry larger or equal to the key.
// If exactp is non-null, stores whether the found entry was an exact match
// in *exactp (1 or 0).
// Updates the cursor index with the index of the found entry.
// If no entry larger or equal to the key is found, returns NULL.
func (c *cursor) search(key []byte) (*node, bool) {
	/*
	unsigned int	 i = 0, nkeys;
	int		 low, high;
	int		 rc = 0;
	MDB_page *mp = mc->mc_pg[mc->mc_top];
	MDB_node	*node = NULL;
	MDB_val	 nodekey;
	MDB_cmp_func *cmp;
	DKBUF;

	nkeys = NUMKEYS(mp);

	DPRINTF(("searching %u keys in %s %spage %"Z"u",
	    nkeys, IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "",
	    mdb_dbg_pgno(mp)));

	low = IS_LEAF(mp) ? 0 : 1;
	high = nkeys - 1;
	cmp = mc->mc_dbx->md_cmp;

	// Branch pages have no data, so if using integer keys,
	// alignment is guaranteed. Use faster mdb_cmp_int.
	if (cmp == mdb_cmp_cint && IS_BRANCH(mp)) {
		if (NODEPTR(mp, 1)->mn_ksize == sizeof(size_t))
			cmp = mdb_cmp_long;
		else
			cmp = mdb_cmp_int;
	}

	if (IS_LEAF2(mp)) {
		nodekey.mv_size = mc->mc_db->md_pad;
		node = NODEPTR(mp, 0);	// fake
		while (low <= high) {
			i = (low + high) >> 1;
			nodekey.mv_data = LEAF2KEY(mp, i, nodekey.mv_size);
			rc = cmp(key, &nodekey);
			DPRINTF(("found leaf index %u [%s], rc = %i",
			    i, DKEY(&nodekey), rc));
			if (rc == 0)
				break;
			if (rc > 0)
				low = i + 1;
			else
				high = i - 1;
		}
	} else {
		while (low <= high) {
			i = (low + high) >> 1;

			node = NODEPTR(mp, i);
			nodekey.mv_size = NODEKSZ(node);
			nodekey.mv_data = NODEKEY(node);

			rc = cmp(key, &nodekey);
#if MDB_DEBUG
			if (IS_LEAF(mp))
				DPRINTF(("found leaf index %u [%s], rc = %i",
				    i, DKEY(&nodekey), rc));
			else
				DPRINTF(("found branch index %u [%s -> %"Z"u], rc = %i",
				    i, DKEY(&nodekey), NODEPGNO(node), rc));
#endif
			if (rc == 0)
				break;
			if (rc > 0)
				low = i + 1;
			else
				high = i - 1;
		}
	}

	if (rc > 0) {	// Found entry is less than the key.
		i++;	// Skip to get the smallest entry larger than key.
		if (!IS_LEAF2(mp))
			node = NODEPTR(mp, i);
	}
	if (exactp)
		*exactp = (rc == 0 && nkeys > 0);
	// store the key index
	mc->mc_ki[mc->mc_top] = i;
	if (i >= nkeys)
		// There is no entry larger or equal to the key.
		return NULL;

	// nodeptr is fake for LEAF2
	return node;
	*/
	return nil, false
}

func (c *cursor) pop() {
	/*
	if (mc->mc_snum) {
#if MDB_DEBUG
		MDB_page	*top = mc->mc_pg[mc->mc_top];
#endif
		mc->mc_snum--;
		if (mc->mc_snum)
			mc->mc_top--;

		DPRINTF(("popped page %"Z"u off db %d cursor %p", top->mp_pgno,
			DDBI(mc), (void *) mc));
	}
	*/
}

/** Push a page onto the top of the cursor's stack. */
func (c *cursor) push(p *page) error {
	/*
	DPRINTF(("pushing page %"Z"u on db %d cursor %p", mp->mp_pgno,
		DDBI(mc), (void *) mc));

	if (mc->mc_snum >= CURSOR_STACK) {
		mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
		return MDB_CURSOR_FULL;
	}

	mc->mc_top = mc->mc_snum++;
	mc->mc_pg[mc->mc_top] = mp;
	mc->mc_ki[mc->mc_top] = 0;

	return MDB_SUCCESS;
	*/
	return nil
}

// Finish #mdb_page_search() / #mdb_page_search_lowest().
// The cursor is at the root page, set up the rest of it.
func (c *cursor) searchRoot(key []byte, flags int) error {
	/*
	MDB_page	*mp = mc->mc_pg[mc->mc_top];
	int rc;
	DKBUF;

	while (IS_BRANCH(mp)) {
		MDB_node	*node;
		indx_t		i;

		DPRINTF(("branch page %"Z"u has %u keys", mp->mp_pgno, NUMKEYS(mp)));
		mdb_cassert(mc, NUMKEYS(mp) > 1);
		DPRINTF(("found index 0 to page %"Z"u", NODEPGNO(NODEPTR(mp, 0))));

		if (flags & (MDB_PS_FIRST|MDB_PS_LAST)) {
			i = 0;
			if (flags & MDB_PS_LAST)
				i = NUMKEYS(mp) - 1;
		} else {
			int	 exact;
			node = mdb_node_search(mc, key, &exact);
			if (node == NULL)
				i = NUMKEYS(mp) - 1;
			else {
				i = mc->mc_ki[mc->mc_top];
				if (!exact) {
					mdb_cassert(mc, i > 0);
					i--;
				}
			}
			DPRINTF(("following index %u for key [%s]", i, DKEY(key)));
		}

		mdb_cassert(mc, i < NUMKEYS(mp));
		node = NODEPTR(mp, i);

		if ((rc = mdb_page_get(mc->mc_txn, NODEPGNO(node), &mp, NULL)) != 0)
			return rc;

		mc->mc_ki[mc->mc_top] = i;
		if ((rc = mdb_cursor_push(mc, mp)))
			return rc;

		if (flags & MDB_PS_MODIFY) {
			if ((rc = mdb_page_touch(mc)) != 0)
				return rc;
			mp = mc->mc_pg[mc->mc_top];
		}
	}

	if (!IS_LEAF(mp)) {
		DPRINTF(("internal error, index points to a %02X page!?",
		    mp->mp_flags));
		mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
		return MDB_CORRUPTED;
	}

	DPRINTF(("found leaf page %"Z"u for key [%s]", mp->mp_pgno,
	    key ? DKEY(key) : "null"));
	mc->mc_flags |= C_INITIALIZED;
	mc->mc_flags &= ~C_EOF;

	return MDB_SUCCESS;
	*/
	return nil
}

// Search for the lowest key under the current branch page.
// This just bypasses a NUMKEYS check in the current page
// before calling mdb_page_search_root(), because the callers
// are all in situations where the current page is known to
// be underfilled.
func (c *cursor) searchLowest() error {
	/*
	MDB_page	*mp = mc->mc_pg[mc->mc_top];
	MDB_node	*node = NODEPTR(mp, 0);
	int rc;

	if ((rc = mdb_page_get(mc->mc_txn, NODEPGNO(node), &mp, NULL)) != 0)
		return rc;

	mc->mc_ki[mc->mc_top] = 0;
	if ((rc = mdb_cursor_push(mc, mp)))
		return rc;
	return mdb_page_search_root(mc, NULL, MDB_PS_FIRST);
	*/
}

// Search for the page a given key should be in.
// Push it and its parent pages on the cursor stack.
// @param[in,out] mc the cursor for this operation.
// @param[in] key the key to search for, or NULL for first/last page.
// @param[in] flags If MDB_PS_MODIFY is set, visited pages in the DB
//   are touched (updated with new page numbers).
//   If MDB_PS_FIRST or MDB_PS_LAST is set, find first or last leaf.
//   This is used by #mdb_cursor_first() and #mdb_cursor_last().
//   If MDB_PS_ROOTONLY set, just fetch root node, no further lookups.
// @return 0 on success, non-zero on failure.
func (c *cursor) findPage(key []byte, flags int) error {
	/*
	int		 rc;
	pgno_t		 root;

	// Make sure the txn is still viable, then find the root from
	// the txn's db table and set it as the root of the cursor's stack.
	if (F_ISSET(mc->mc_txn->mt_flags, MDB_TXN_ERROR)) {
		DPUTS("transaction has failed, must abort");
		return MDB_BAD_TXN;
	} else {
		// Make sure we're using an up-to-date root
		if (*mc->mc_dbflag & DB_STALE) {
				MDB_cursor mc2;
				mdb_cursor_init(&mc2, mc->mc_txn, MAIN_DBI, NULL);
				rc = mdb_page_search(&mc2, &mc->mc_dbx->md_name, 0);
				if (rc)
					return rc;
				{
					MDB_val data;
					int exact = 0;
					uint16_t flags;
					MDB_node *leaf = mdb_node_search(&mc2,
						&mc->mc_dbx->md_name, &exact);
					if (!exact)
						return MDB_NOTFOUND;
					rc = mdb_node_read(mc->mc_txn, leaf, &data);
					if (rc)
						return rc;
					memcpy(&flags, ((char *) data.mv_data + offsetof(MDB_db, md_flags)),
						sizeof(uint16_t));
					// The txn may not know this DBI, or another process may
					// have dropped and recreated the DB with other flags.
					if ((mc->mc_db->md_flags & PERSISTENT_FLAGS) != flags)
						return MDB_INCOMPATIBLE;
					memcpy(mc->mc_db, data.mv_data, sizeof(MDB_db));
				}
				*mc->mc_dbflag &= ~DB_STALE;
		}
		root = mc->mc_db->md_root;

		if (root == P_INVALID) {		// Tree is empty.
			DPUTS("tree is empty");
			return MDB_NOTFOUND;
		}
	}

	mdb_cassert(mc, root > 1);
	if (!mc->mc_pg[0] || mc->mc_pg[0]->mp_pgno != root)
		if ((rc = mdb_page_get(mc->mc_txn, root, &mc->mc_pg[0], NULL)) != 0)
			return rc;

	mc->mc_snum = 1;
	mc->mc_top = 0;

	DPRINTF(("db %d root page %"Z"u has flags 0x%X",
		DDBI(mc), root, mc->mc_pg[0]->mp_flags));

	if (flags & MDB_PS_MODIFY) {
		if ((rc = mdb_page_touch(mc)))
			return rc;
	}

	if (flags & MDB_PS_ROOTONLY)
		return MDB_SUCCESS;

	return mdb_page_search_root(mc, key, flags);
	*/
	return nil
}

func (c *cursor) freeOverflowPage(p *page) error {
	/*
	MDB_txn *txn = mc->mc_txn;
	pgno_t pg = mp->mp_pgno;
	unsigned x = 0, ovpages = mp->mp_pages;
	MDB_env *env = txn->mt_env;
	MDB_IDL sl = txn->mt_spill_pgs;
	MDB_ID pn = pg << 1;
	int rc;

	DPRINTF(("free ov page %"Z"u (%d)", pg, ovpages));
	// If the page is dirty or on the spill list we just acquired it,
	// so we should give it back to our current free list, if any.
	// Otherwise put it onto the list of pages we freed in this txn.
	//
	// Won't create me_pghead: me_pglast must be inited along with it.
	// Unsupported in nested txns: They would need to hide the page
	// range in ancestor txns' dirty and spilled lists.
	if (env->me_pghead &&
		!txn->mt_parent &&
		((mp->mp_flags & P_DIRTY) ||
		 (sl && (x = mdb_midl_search(sl, pn)) <= sl[0] && sl[x] == pn)))
	{
		unsigned i, j;
		pgno_t *mop;
		MDB_ID2 *dl, ix, iy;
		rc = mdb_midl_need(&env->me_pghead, ovpages);
		if (rc)
			return rc;
		if (!(mp->mp_flags & P_DIRTY)) {
			// This page is no longer spilled
			if (x == sl[0])
				sl[0]--;
			else
				sl[x] |= 1;
			goto release;
		}
		// Remove from dirty list
		dl = txn->mt_u.dirty_list;
		x = dl[0].mid--;
		for (ix = dl[x]; ix.mptr != mp; ix = iy) {
			if (x > 1) {
				x--;
				iy = dl[x];
				dl[x] = ix;
			} else {
				mdb_cassert(mc, x > 1);
				j = ++(dl[0].mid);
				dl[j] = ix;		// Unsorted. OK when MDB_TXN_ERROR.
				txn->mt_flags |= MDB_TXN_ERROR;
				return MDB_CORRUPTED;
			}
		}
		if (!(env->me_flags & MDB_WRITEMAP))
			mdb_dpage_free(env, mp);
release:
		// Insert in me_pghead
		mop = env->me_pghead;
		j = mop[0] + ovpages;
		for (i = mop[0]; i && mop[i] < pg; i--)
			mop[j--] = mop[i];
		while (j>i)
			mop[j--] = pg++;
		mop[0] += ovpages;
	} else {
		rc = mdb_midl_append_range(&txn->mt_free_pgs, pg, ovpages);
		if (rc)
			return rc;
	}
	mc->mc_db->md_overflow_pages -= ovpages;
	return 0;
	*/
	return nil
}


// Find a sibling for a page.
// Replaces the page at the top of the cursor's stack with the
// specified sibling, if one exists.
// @param[in] mc The cursor for this operation.
// @param[in] move_right Non-zero if the right sibling is requested,
// otherwise the left sibling.
// @return 0 on success, non-zero on failure.
func (c *cursor) sibling(moveRight bool) error {
	/*
	int		 rc;
	MDB_node	*indx;
	MDB_page	*mp;

	if (mc->mc_snum < 2) {
		return MDB_NOTFOUND;		// root has no siblings
	}

	mdb_cursor_pop(mc);
	DPRINTF(("parent page is page %"Z"u, index %u",
		mc->mc_pg[mc->mc_top]->mp_pgno, mc->mc_ki[mc->mc_top]));

	if (move_right ? (mc->mc_ki[mc->mc_top] + 1u >= NUMKEYS(mc->mc_pg[mc->mc_top]))
		       : (mc->mc_ki[mc->mc_top] == 0)) {
		DPRINTF(("no more keys left, moving to %s sibling",
		    move_right ? "right" : "left"));
		if ((rc = mdb_cursor_sibling(mc, move_right)) != MDB_SUCCESS) {
			// undo cursor_pop before returning
			mc->mc_top++;
			mc->mc_snum++;
			return rc;
		}
	} else {
		if (move_right)
			mc->mc_ki[mc->mc_top]++;
		else
			mc->mc_ki[mc->mc_top]--;
		DPRINTF(("just moving to %s index key %u",
		    move_right ? "right" : "left", mc->mc_ki[mc->mc_top]));
	}
	mdb_cassert(mc, IS_BRANCH(mc->mc_pg[mc->mc_top]));

	indx = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
	if ((rc = mdb_page_get(mc->mc_txn, NODEPGNO(indx), &mp, NULL)) != 0) {
		// mc will be inconsistent if caller does mc_snum++ as above
		mc->mc_flags &= ~(C_INITIALIZED|C_EOF);
		return rc;
	}

	mdb_cursor_push(mc, mp);
	if (!move_right)
		mc->mc_ki[mc->mc_top] = NUMKEYS(mp)-1;

	return MDB_SUCCESS;
	*/
	return nil
}

// Move the cursor to the next data item.
func (c *cursor) next(key []byte, data []byte, op int) error {
	/*
	MDB_page	*mp;
	MDB_node	*leaf;
	int rc;

	if (mc->mc_flags & C_EOF) {
		return MDB_NOTFOUND;
	}

	mdb_cassert(mc, mc->mc_flags & C_INITIALIZED);

	mp = mc->mc_pg[mc->mc_top];

	if (mc->mc_db->md_flags & MDB_DUPSORT) {
		leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			if (op == MDB_NEXT || op == MDB_NEXT_DUP) {
				rc = mdb_cursor_next(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_NEXT);
				if (op != MDB_NEXT || rc != MDB_NOTFOUND) {
					if (rc == MDB_SUCCESS)
						MDB_GET_KEY(leaf, key);
					return rc;
				}
			}
		} else {
			mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);
			if (op == MDB_NEXT_DUP)
				return MDB_NOTFOUND;
		}
	}

	DPRINTF(("cursor_next: top page is %"Z"u in cursor %p",
		mdb_dbg_pgno(mp), (void *) mc));
	if (mc->mc_flags & C_DEL)
		goto skip;

	if (mc->mc_ki[mc->mc_top] + 1u >= NUMKEYS(mp)) {
		DPUTS("=====> move to next sibling page");
		if ((rc = mdb_cursor_sibling(mc, 1)) != MDB_SUCCESS) {
			mc->mc_flags |= C_EOF;
			return rc;
		}
		mp = mc->mc_pg[mc->mc_top];
		DPRINTF(("next page is %"Z"u, key index %u", mp->mp_pgno, mc->mc_ki[mc->mc_top]));
	} else
		mc->mc_ki[mc->mc_top]++;

skip:
	DPRINTF(("==> cursor points to page %"Z"u with %u keys, key index %u",
	    mdb_dbg_pgno(mp), NUMKEYS(mp), mc->mc_ki[mc->mc_top]));

	if (IS_LEAF2(mp)) {
		key->mv_size = mc->mc_db->md_pad;
		key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
		return MDB_SUCCESS;
	}

	mdb_cassert(mc, IS_LEAF(mp));
	leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

	if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
		mdb_xcursor_init1(mc, leaf);
	}
	if (data) {
		if ((rc = mdb_node_read(mc->mc_txn, leaf, data)) != MDB_SUCCESS)
			return rc;

		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			rc = mdb_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
			if (rc != MDB_SUCCESS)
				return rc;
		}
	}

	MDB_GET_KEY(leaf, key);
	return MDB_SUCCESS;
	*/
	return nil
}

// Move the cursor to the previous data item.
func (c *cursor) prev(key []byte, data []byte, op int) error {
	/*
	MDB_page	*mp;
	MDB_node	*leaf;
	int rc;

	mdb_cassert(mc, mc->mc_flags & C_INITIALIZED);

	mp = mc->mc_pg[mc->mc_top];

	if (mc->mc_db->md_flags & MDB_DUPSORT) {
		leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			if (op == MDB_PREV || op == MDB_PREV_DUP) {
				rc = mdb_cursor_prev(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_PREV);
				if (op != MDB_PREV || rc != MDB_NOTFOUND) {
					if (rc == MDB_SUCCESS)
						MDB_GET_KEY(leaf, key);
					return rc;
				}
			} else {
				mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);
				if (op == MDB_PREV_DUP)
					return MDB_NOTFOUND;
			}
		}
	}

	DPRINTF(("cursor_prev: top page is %"Z"u in cursor %p",
		mdb_dbg_pgno(mp), (void *) mc));

	if (mc->mc_ki[mc->mc_top] == 0)  {
		DPUTS("=====> move to prev sibling page");
		if ((rc = mdb_cursor_sibling(mc, 0)) != MDB_SUCCESS) {
			return rc;
		}
		mp = mc->mc_pg[mc->mc_top];
		mc->mc_ki[mc->mc_top] = NUMKEYS(mp) - 1;
		DPRINTF(("prev page is %"Z"u, key index %u", mp->mp_pgno, mc->mc_ki[mc->mc_top]));
	} else
		mc->mc_ki[mc->mc_top]--;

	mc->mc_flags &= ~C_EOF;

	DPRINTF(("==> cursor points to page %"Z"u with %u keys, key index %u",
	    mdb_dbg_pgno(mp), NUMKEYS(mp), mc->mc_ki[mc->mc_top]));

	if (IS_LEAF2(mp)) {
		key->mv_size = mc->mc_db->md_pad;
		key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
		return MDB_SUCCESS;
	}

	mdb_cassert(mc, IS_LEAF(mp));
	leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

	if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
		mdb_xcursor_init1(mc, leaf);
	}
	if (data) {
		if ((rc = mdb_node_read(mc->mc_txn, leaf, data)) != MDB_SUCCESS)
			return rc;

		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			rc = mdb_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
			if (rc != MDB_SUCCESS)
				return rc;
		}
	}

	MDB_GET_KEY(leaf, key);
	return MDB_SUCCESS;
	*/
}

// Set the cursor on a specific data item.
// (bool return is whether it is exact).
func (c *cursor) set(key []byte, data []byte, op int) (error, bool) {
	/*
	int		 rc;
	MDB_page	*mp;
	MDB_node	*leaf = NULL;
	DKBUF;

	if (key->mv_size == 0)
		return MDB_BAD_VALSIZE;

	if (mc->mc_xcursor)
		mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);

	// See if we're already on the right page
	if (mc->mc_flags & C_INITIALIZED) {
		MDB_val nodekey;

		mp = mc->mc_pg[mc->mc_top];
		if (!NUMKEYS(mp)) {
			mc->mc_ki[mc->mc_top] = 0;
			return MDB_NOTFOUND;
		}
		if (mp->mp_flags & P_LEAF2) {
			nodekey.mv_size = mc->mc_db->md_pad;
			nodekey.mv_data = LEAF2KEY(mp, 0, nodekey.mv_size);
		} else {
			leaf = NODEPTR(mp, 0);
			MDB_GET_KEY2(leaf, nodekey);
		}
		rc = mc->mc_dbx->md_cmp(key, &nodekey);
		if (rc == 0) {
			// Probably happens rarely, but first node on the page
			// was the one we wanted.
			mc->mc_ki[mc->mc_top] = 0;
			if (exactp)
				*exactp = 1;
			goto set1;
		}
		if (rc > 0) {
			unsigned int i;
			unsigned int nkeys = NUMKEYS(mp);
			if (nkeys > 1) {
				if (mp->mp_flags & P_LEAF2) {
					nodekey.mv_data = LEAF2KEY(mp,
						 nkeys-1, nodekey.mv_size);
				} else {
					leaf = NODEPTR(mp, nkeys-1);
					MDB_GET_KEY2(leaf, nodekey);
				}
				rc = mc->mc_dbx->md_cmp(key, &nodekey);
				if (rc == 0) {
					// last node was the one we wanted
					mc->mc_ki[mc->mc_top] = nkeys-1;
					if (exactp)
						*exactp = 1;
					goto set1;
				}
				if (rc < 0) {
					if (mc->mc_ki[mc->mc_top] < NUMKEYS(mp)) {
						// This is definitely the right page, skip search_page 
						if (mp->mp_flags & P_LEAF2) {
							nodekey.mv_data = LEAF2KEY(mp,
								 mc->mc_ki[mc->mc_top], nodekey.mv_size);
						} else {
							leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
							MDB_GET_KEY2(leaf, nodekey);
						}
						rc = mc->mc_dbx->md_cmp(key, &nodekey);
						if (rc == 0) {
							// current node was the one we wanted
							if (exactp)
								*exactp = 1;
							goto set1;
						}
					}
					rc = 0;
					goto set2;
				}
			}
			// If any parents have right-sibs, search.
			// Otherwise, there's nothing further.
			for (i=0; i<mc->mc_top; i++)
				if (mc->mc_ki[i] <
					NUMKEYS(mc->mc_pg[i])-1)
					break;
			if (i == mc->mc_top) {
				// There are no other pages
				mc->mc_ki[mc->mc_top] = nkeys;
				return MDB_NOTFOUND;
			}
		}
		if (!mc->mc_top) {
			// There are no other pages
			mc->mc_ki[mc->mc_top] = 0;
			if (op == MDB_SET_RANGE) {
				rc = 0;
				goto set1;
			} else
				return MDB_NOTFOUND;
		}
	}

	rc = mdb_page_search(mc, key, 0);
	if (rc != MDB_SUCCESS)
		return rc;

	mp = mc->mc_pg[mc->mc_top];
	mdb_cassert(mc, IS_LEAF(mp));

set2:
	leaf = mdb_node_search(mc, key, exactp);
	if (exactp != NULL && !*exactp) {
		// MDB_SET specified and not an exact match.
		return MDB_NOTFOUND;
	}

	if (leaf == NULL) {
		DPUTS("===> inexact leaf not found, goto sibling");
		if ((rc = mdb_cursor_sibling(mc, 1)) != MDB_SUCCESS)
			return rc;		// no entries matched
		mp = mc->mc_pg[mc->mc_top];
		mdb_cassert(mc, IS_LEAF(mp));
		leaf = NODEPTR(mp, 0);
	}

set1:
	mc->mc_flags |= C_INITIALIZED;
	mc->mc_flags &= ~C_EOF;

	if (IS_LEAF2(mp)) {
		key->mv_size = mc->mc_db->md_pad;
		key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
		return MDB_SUCCESS;
	}

	if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
		mdb_xcursor_init1(mc, leaf);
	}
	if (data) {
		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			if (op == MDB_SET || op == MDB_SET_KEY || op == MDB_SET_RANGE) {
				rc = mdb_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
			} else {
				int ex2, *ex2p;
				if (op == MDB_GET_BOTH) {
					ex2p = &ex2;
					ex2 = 0;
				} else {
					ex2p = NULL;
				}
				rc = mdb_cursor_set(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_SET_RANGE, ex2p);
				if (rc != MDB_SUCCESS)
					return rc;
			}
		} else if (op == MDB_GET_BOTH || op == MDB_GET_BOTH_RANGE) {
			MDB_val d2;
			if ((rc = mdb_node_read(mc->mc_txn, leaf, &d2)) != MDB_SUCCESS)
				return rc;
			rc = mc->mc_dbx->md_dcmp(data, &d2);
			if (rc) {
				if (op == MDB_GET_BOTH || rc > 0)
					return MDB_NOTFOUND;
				rc = 0;
				*data = d2;
			}

		} else {
			if (mc->mc_xcursor)
				mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);
			if ((rc = mdb_node_read(mc->mc_txn, leaf, data)) != MDB_SUCCESS)
				return rc;
		}
	}

	// The key already matches in all other cases
	if (op == MDB_SET_RANGE || op == MDB_SET_KEY)
		MDB_GET_KEY(leaf, key);
	DPRINTF(("==> cursor placed on key [%s]", DKEY(key)));

	return rc;
	*/

	return nil
}

// Move the cursor to the first item in the database.
func (c *cursor) first(key []byte, data []byte) error {
	/*
	int		 rc;
	MDB_node	*leaf;

	if (mc->mc_xcursor)
		mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);

	if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
		rc = mdb_page_search(mc, NULL, MDB_PS_FIRST);
		if (rc != MDB_SUCCESS)
			return rc;
	}
	mdb_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));

	leaf = NODEPTR(mc->mc_pg[mc->mc_top], 0);
	mc->mc_flags |= C_INITIALIZED;
	mc->mc_flags &= ~C_EOF;

	mc->mc_ki[mc->mc_top] = 0;

	if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
		key->mv_size = mc->mc_db->md_pad;
		key->mv_data = LEAF2KEY(mc->mc_pg[mc->mc_top], 0, key->mv_size);
		return MDB_SUCCESS;
	}

	if (data) {
		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			mdb_xcursor_init1(mc, leaf);
			rc = mdb_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
			if (rc)
				return rc;
		} else {
			if ((rc = mdb_node_read(mc->mc_txn, leaf, data)) != MDB_SUCCESS)
				return rc;
		}
	}
	MDB_GET_KEY(leaf, key);
	return MDB_SUCCESS;
	*/
	return nil
}

// Move the cursor to the last item in the database.
func (c *cursor) last() ([]byte, []byte) {
	int		 rc;
	MDB_node	*leaf;

	if (mc->mc_xcursor)
		mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED|C_EOF);

	if (!(mc->mc_flags & C_EOF)) {

		if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
			rc = mdb_page_search(mc, NULL, MDB_PS_LAST);
			if (rc != MDB_SUCCESS)
				return rc;
		}
		mdb_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));

	}
	mc->mc_ki[mc->mc_top] = NUMKEYS(mc->mc_pg[mc->mc_top]) - 1;
	mc->mc_flags |= C_INITIALIZED|C_EOF;
	leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);

	if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
		key->mv_size = mc->mc_db->md_pad;
		key->mv_data = LEAF2KEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], key->mv_size);
		return MDB_SUCCESS;
	}

	if (data) {
		if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
			mdb_xcursor_init1(mc, leaf);
			rc = mdb_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
			if (rc)
				return rc;
		} else {
			if ((rc = mdb_node_read(mc->mc_txn, leaf, data)) != MDB_SUCCESS)
				return rc;
		}
	}

	MDB_GET_KEY(leaf, key);
	return MDB_SUCCESS;
}

func (c *cursor) Get(key []byte, data []byte, op int) ([]byte, []byte, error) {
	/*
	int		 rc;
	int		 exact = 0;
	int		 (*mfunc)(MDB_cursor *mc, MDB_val *key, MDB_val *data);

	if (mc == NULL)
		return EINVAL;

	if (mc->mc_txn->mt_flags & MDB_TXN_ERROR)
		return MDB_BAD_TXN;

	switch (op) {
	case MDB_GET_CURRENT:
		if (!(mc->mc_flags & C_INITIALIZED)) {
			rc = EINVAL;
		} else {
			MDB_page *mp = mc->mc_pg[mc->mc_top];
			int nkeys = NUMKEYS(mp);
			if (!nkeys || mc->mc_ki[mc->mc_top] >= nkeys) {
				mc->mc_ki[mc->mc_top] = nkeys;
				rc = MDB_NOTFOUND;
				break;
			}
			rc = MDB_SUCCESS;
			if (IS_LEAF2(mp)) {
				key->mv_size = mc->mc_db->md_pad;
				key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
			} else {
				MDB_node *leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
				MDB_GET_KEY(leaf, key);
				if (data) {
					if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
						if (mc->mc_flags & C_DEL)
							mdb_xcursor_init1(mc, leaf);
						rc = mdb_cursor_get(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_GET_CURRENT);
					} else {
						rc = mdb_node_read(mc->mc_txn, leaf, data);
					}
				}
			}
		}
		break;
	case MDB_GET_BOTH:
	case MDB_GET_BOTH_RANGE:
		if (data == NULL) {
			rc = EINVAL;
			break;
		}
		if (mc->mc_xcursor == NULL) {
			rc = MDB_INCOMPATIBLE;
			break;
		}
		// FALLTHRU
	case MDB_SET:
	case MDB_SET_KEY:
	case MDB_SET_RANGE:
		if (key == NULL) {
			rc = EINVAL;
		} else {
			rc = mdb_cursor_set(mc, key, data, op,
				op == MDB_SET_RANGE ? NULL : &exact);
		}
		break;
	case MDB_GET_MULTIPLE:
		if (data == NULL || !(mc->mc_flags & C_INITIALIZED)) {
			rc = EINVAL;
			break;
		}
		if (!(mc->mc_db->md_flags & MDB_DUPFIXED)) {
			rc = MDB_INCOMPATIBLE;
			break;
		}
		rc = MDB_SUCCESS;
		if (!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) ||
			(mc->mc_xcursor->mx_cursor.mc_flags & C_EOF))
			break;
		goto fetchm;
	case MDB_NEXT_MULTIPLE:
		if (data == NULL) {
			rc = EINVAL;
			break;
		}
		if (!(mc->mc_db->md_flags & MDB_DUPFIXED)) {
			rc = MDB_INCOMPATIBLE;
			break;
		}
		if (!(mc->mc_flags & C_INITIALIZED))
			rc = mdb_cursor_first(mc, key, data);
		else
			rc = mdb_cursor_next(mc, key, data, MDB_NEXT_DUP);
		if (rc == MDB_SUCCESS) {
			if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
				MDB_cursor *mx;
fetchm:
				mx = &mc->mc_xcursor->mx_cursor;
				data->mv_size = NUMKEYS(mx->mc_pg[mx->mc_top]) *
					mx->mc_db->md_pad;
				data->mv_data = METADATA(mx->mc_pg[mx->mc_top]);
				mx->mc_ki[mx->mc_top] = NUMKEYS(mx->mc_pg[mx->mc_top])-1;
			} else {
				rc = MDB_NOTFOUND;
			}
		}
		break;
	case MDB_NEXT:
	case MDB_NEXT_DUP:
	case MDB_NEXT_NODUP:
		if (!(mc->mc_flags & C_INITIALIZED))
			rc = mdb_cursor_first(mc, key, data);
		else
			rc = mdb_cursor_next(mc, key, data, op);
		break;
	case MDB_PREV:
	case MDB_PREV_DUP:
	case MDB_PREV_NODUP:
		if (!(mc->mc_flags & C_INITIALIZED)) {
			rc = mdb_cursor_last(mc, key, data);
			if (rc)
				break;
			mc->mc_flags |= C_INITIALIZED;
			mc->mc_ki[mc->mc_top]++;
		}
		rc = mdb_cursor_prev(mc, key, data, op);
		break;
	case MDB_FIRST:
		rc = mdb_cursor_first(mc, key, data);
		break;
	case MDB_FIRST_DUP:
		mfunc = mdb_cursor_first;
	mmove:
		if (data == NULL || !(mc->mc_flags & C_INITIALIZED)) {
			rc = EINVAL;
			break;
		}
		if (mc->mc_xcursor == NULL) {
			rc = MDB_INCOMPATIBLE;
			break;
		}
		if (!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED)) {
			rc = EINVAL;
			break;
		}
		rc = mfunc(&mc->mc_xcursor->mx_cursor, data, NULL);
		break;
	case MDB_LAST:
		rc = mdb_cursor_last(mc, key, data);
		break;
	case MDB_LAST_DUP:
		mfunc = mdb_cursor_last;
		goto mmove;
	default:
		DPRINTF(("unhandled/unimplemented cursor operation %u", op));
		rc = EINVAL;
		break;
	}

	if (mc->mc_flags & C_DEL)
		mc->mc_flags ^= C_DEL;

	return rc;
	*/
	return nil, nil, nil
}

// Touch all the pages in the cursor stack. Set mc_top.
//	Makes sure all the pages are writable, before attempting a write operation.
// @param[in] mc The cursor to operate on.
func (c *cursor) touch() error {
	/*
	int rc = MDB_SUCCESS;

	if (mc->mc_dbi > MAIN_DBI && !(*mc->mc_dbflag & DB_DIRTY)) {
		MDB_cursor mc2;
		MDB_xcursor mcx;
		mdb_cursor_init(&mc2, mc->mc_txn, MAIN_DBI, &mcx);
		rc = mdb_page_search(&mc2, &mc->mc_dbx->md_name, MDB_PS_MODIFY);
		if (rc)
			 return rc;
		*mc->mc_dbflag |= DB_DIRTY;
	}
	mc->mc_top = 0;
	if (mc->mc_snum) {
		do {
			rc = mdb_page_touch(mc);
		} while (!rc && ++(mc->mc_top) < mc->mc_snum);
		mc->mc_top = mc->mc_snum-1;
	}
	return rc;
}

int
mdb_cursor_put(MDB_cursor *mc, MDB_val *key, MDB_val *data,
    unsigned int flags)
{
	enum { MDB_NO_ROOT = MDB_LAST_ERRCODE+10 }; // internal code
	MDB_env		*env;
	MDB_node	*leaf = NULL;
	MDB_page	*fp, *mp;
	uint16_t	fp_flags;
	MDB_val		xdata, *rdata, dkey, olddata;
	MDB_db dummy;
	int do_sub = 0, insert;
	unsigned int mcount = 0, dcount = 0, nospill;
	size_t nsize;
	int rc, rc2;
	unsigned int nflags;
	DKBUF;

	if (mc == NULL || key == NULL)
		return EINVAL;

	env = mc->mc_txn->mt_env;

	// Check this first so counter will always be zero on any
	// early failures.
	if (flags & MDB_MULTIPLE) {
		dcount = data[1].mv_size;
		data[1].mv_size = 0;
		if (!F_ISSET(mc->mc_db->md_flags, MDB_DUPFIXED))
			return MDB_INCOMPATIBLE;
	}

	nospill = flags & MDB_NOSPILL;
	flags &= ~MDB_NOSPILL;

	if (mc->mc_txn->mt_flags & (MDB_TXN_RDONLY|MDB_TXN_ERROR))
		return (mc->mc_txn->mt_flags & MDB_TXN_RDONLY) ? EACCES : MDB_BAD_TXN;

	if (flags != MDB_CURRENT && key->mv_size-1 >= ENV_MAXKEY(env))
		return MDB_BAD_VALSIZE;

#if SIZE_MAX > MAXDATASIZE
	if (data->mv_size > ((mc->mc_db->md_flags & MDB_DUPSORT) ? ENV_MAXKEY(env) : MAXDATASIZE))
		return MDB_BAD_VALSIZE;
#else
	if ((mc->mc_db->md_flags & MDB_DUPSORT) && data->mv_size > ENV_MAXKEY(env))
		return MDB_BAD_VALSIZE;
#endif

	DPRINTF(("==> put db %d key [%s], size %"Z"u, data size %"Z"u",
		DDBI(mc), DKEY(key), key ? key->mv_size : 0, data->mv_size));

	dkey.mv_size = 0;

	if (flags == MDB_CURRENT) {
		if (!(mc->mc_flags & C_INITIALIZED))
			return EINVAL;
		rc = MDB_SUCCESS;
	} else if (mc->mc_db->md_root == P_INVALID) {
		// new database, cursor has nothing to point to
		mc->mc_snum = 0;
		mc->mc_top = 0;
		mc->mc_flags &= ~C_INITIALIZED;
		rc = MDB_NO_ROOT;
	} else {
		int exact = 0;
		MDB_val d2;
		if (flags & MDB_APPEND) {
			MDB_val k2;
			rc = mdb_cursor_last(mc, &k2, &d2);
			if (rc == 0) {
				rc = mc->mc_dbx->md_cmp(key, &k2);
				if (rc > 0) {
					rc = MDB_NOTFOUND;
					mc->mc_ki[mc->mc_top]++;
				} else {
					// new key is <= last key
					rc = MDB_KEYEXIST;
				}
			}
		} else {
			rc = mdb_cursor_set(mc, key, &d2, MDB_SET, &exact);
		}
		if ((flags & MDB_NOOVERWRITE) && rc == 0) {
			DPRINTF(("duplicate key [%s]", DKEY(key)));
			*data = d2;
			return MDB_KEYEXIST;
		}
		if (rc && rc != MDB_NOTFOUND)
			return rc;
	}

	if (mc->mc_flags & C_DEL)
		mc->mc_flags ^= C_DEL;

	// Cursor is positioned, check for room in the dirty list
	if (!nospill) {
		if (flags & MDB_MULTIPLE) {
			rdata = &xdata;
			xdata.mv_size = data->mv_size * dcount;
		} else {
			rdata = data;
		}
		if ((rc2 = mdb_page_spill(mc, key, rdata)))
			return rc2;
	}

	if (rc == MDB_NO_ROOT) {
		MDB_page *np;
		// new database, write a root leaf page
		DPUTS("allocating new root leaf page");
		if ((rc2 = mdb_page_new(mc, P_LEAF, 1, &np))) {
			return rc2;
		}
		mdb_cursor_push(mc, np);
		mc->mc_db->md_root = np->mp_pgno;
		mc->mc_db->md_depth++;
		*mc->mc_dbflag |= DB_DIRTY;
		if ((mc->mc_db->md_flags & (MDB_DUPSORT|MDB_DUPFIXED))
			== MDB_DUPFIXED)
			np->mp_flags |= P_LEAF2;
		mc->mc_flags |= C_INITIALIZED;
	} else {
		// make sure all cursor pages are writable
		rc2 = mdb_cursor_touch(mc);
		if (rc2)
			return rc2;
	}

	insert = rc;
	if (insert) {
		// The key does not exist
		DPRINTF(("inserting key at index %i", mc->mc_ki[mc->mc_top]));
		if ((mc->mc_db->md_flags & MDB_DUPSORT) &&
			LEAFSIZE(key, data) > env->me_nodemax)
		{
			// Too big for a node, insert in sub-DB
			fp_flags = P_LEAF|P_DIRTY;
			fp = env->me_pbuf;
			fp->mp_pad = data->mv_size; // used if MDB_DUPFIXED
			fp->mp_lower = fp->mp_upper = olddata.mv_size = PAGEHDRSZ;
			goto prep_subDB;
		}
	} else {
		// there's only a key anyway, so this is a no-op
		if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
			unsigned int ksize = mc->mc_db->md_pad;
			if (key->mv_size != ksize)
				return MDB_BAD_VALSIZE;
			if (flags == MDB_CURRENT) {
				char *ptr = LEAF2KEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], ksize);
				memcpy(ptr, key->mv_data, ksize);
			}
			return MDB_SUCCESS;
		}

more:
		leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
		olddata.mv_size = NODEDSZ(leaf);
		olddata.mv_data = NODEDATA(leaf);

		// DB has dups?
		if (F_ISSET(mc->mc_db->md_flags, MDB_DUPSORT)) {
			// Prepare (sub-)page/sub-DB to accept the new item,
			// if needed.  fp: old sub-page or a header faking
			// it.  mp: new (sub-)page.  offset: growth in page
			// size.  xdata: node data with new page or DB.
			ssize_t		i, offset = 0;
			mp = fp = xdata.mv_data = env->me_pbuf;
			mp->mp_pgno = mc->mc_pg[mc->mc_top]->mp_pgno;

			// Was a single item before, must convert now
			if (!F_ISSET(leaf->mn_flags, F_DUPDATA)) {
				// Just overwrite the current item
				if (flags == MDB_CURRENT)
					goto current;

#if UINT_MAX < SIZE_MAX
				if (mc->mc_dbx->md_dcmp == mdb_cmp_int && olddata.mv_size == sizeof(size_t))
#ifdef MISALIGNED_OK
					mc->mc_dbx->md_dcmp = mdb_cmp_long;
#else
					mc->mc_dbx->md_dcmp = mdb_cmp_cint;
#endif
#endif
				// if data matches, skip it
				if (!mc->mc_dbx->md_dcmp(data, &olddata)) {
					if (flags & MDB_NODUPDATA)
						rc = MDB_KEYEXIST;
					else if (flags & MDB_MULTIPLE)
						goto next_mult;
					else
						rc = MDB_SUCCESS;
					return rc;
				}

				// Back up original data item
				dkey.mv_size = olddata.mv_size;
				dkey.mv_data = memcpy(fp+1, olddata.mv_data, olddata.mv_size);

				// Make sub-page header for the dup items, with dummy body
				fp->mp_flags = P_LEAF|P_DIRTY|P_SUBP;
				fp->mp_lower = PAGEHDRSZ;
				xdata.mv_size = PAGEHDRSZ + dkey.mv_size + data->mv_size;
				if (mc->mc_db->md_flags & MDB_DUPFIXED) {
					fp->mp_flags |= P_LEAF2;
					fp->mp_pad = data->mv_size;
					xdata.mv_size += 2 * data->mv_size;	// leave space for 2 more
				} else {
					xdata.mv_size += 2 * (sizeof(indx_t) + NODESIZE) +
						(dkey.mv_size & 1) + (data->mv_size & 1);
				}
				fp->mp_upper = xdata.mv_size;
				olddata.mv_size = fp->mp_upper; // pretend olddata is fp
			} else if (leaf->mn_flags & F_SUBDATA) {
				// Data is on sub-DB, just store it
				flags |= F_DUPDATA|F_SUBDATA;
				goto put_sub;
			} else {
				// Data is on sub-page
				fp = olddata.mv_data;
				switch (flags) {
				default:
					i = -(ssize_t)SIZELEFT(fp);
					if (!(mc->mc_db->md_flags & MDB_DUPFIXED)) {
						offset = i += (ssize_t) EVEN(
							sizeof(indx_t) + NODESIZE + data->mv_size);
					} else {
						i += offset = fp->mp_pad;
						offset *= 4; // space for 4 more
					}
					if (i > 0)
						break;
					// FALLTHRU: Sub-page is big enough
				case MDB_CURRENT:
					fp->mp_flags |= P_DIRTY;
					COPY_PGNO(fp->mp_pgno, mp->mp_pgno);
					mc->mc_xcursor->mx_cursor.mc_pg[0] = fp;
					flags |= F_DUPDATA;
					goto put_sub;
				}
				xdata.mv_size = olddata.mv_size + offset;
			}

			fp_flags = fp->mp_flags;
			if (NODESIZE + NODEKSZ(leaf) + xdata.mv_size > env->me_nodemax) {
					// Too big for a sub-page, convert to sub-DB
					fp_flags &= ~P_SUBP;
prep_subDB:
					if (mc->mc_db->md_flags & MDB_DUPFIXED) {
						fp_flags |= P_LEAF2;
						dummy.md_pad = fp->mp_pad;
						dummy.md_flags = MDB_DUPFIXED;
						if (mc->mc_db->md_flags & MDB_INTEGERDUP)
							dummy.md_flags |= MDB_INTEGERKEY;
					} else {
						dummy.md_pad = 0;
						dummy.md_flags = 0;
					}
					dummy.md_depth = 1;
					dummy.md_branch_pages = 0;
					dummy.md_leaf_pages = 1;
					dummy.md_overflow_pages = 0;
					dummy.md_entries = NUMKEYS(fp);
					xdata.mv_size = sizeof(MDB_db);
					xdata.mv_data = &dummy;
					if ((rc = mdb_page_alloc(mc, 1, &mp)))
						return rc;
					offset = env->me_psize - olddata.mv_size;
					flags |= F_DUPDATA|F_SUBDATA;
					dummy.md_root = mp->mp_pgno;
			}
			if (mp != fp) {
				mp->mp_flags = fp_flags | P_DIRTY;
				mp->mp_pad   = fp->mp_pad;
				mp->mp_lower = fp->mp_lower;
				mp->mp_upper = fp->mp_upper + offset;
				if (fp_flags & P_LEAF2) {
					memcpy(METADATA(mp), METADATA(fp), NUMKEYS(fp) * fp->mp_pad);
				} else {
					memcpy((char *)mp + mp->mp_upper, (char *)fp + fp->mp_upper,
						olddata.mv_size - fp->mp_upper);
					for (i = NUMKEYS(fp); --i >= 0; )
						mp->mp_ptrs[i] = fp->mp_ptrs[i] + offset;
				}
			}

			rdata = &xdata;
			flags |= F_DUPDATA;
			do_sub = 1;
			if (!insert)
				mdb_node_del(mc, 0);
			goto new_sub;
		}
current:
		// overflow page overwrites need special handling
		if (F_ISSET(leaf->mn_flags, F_BIGDATA)) {
			MDB_page *omp;
			pgno_t pg;
			int level, ovpages, dpages = OVPAGES(data->mv_size, env->me_psize);

			memcpy(&pg, olddata.mv_data, sizeof(pg));
			if ((rc2 = mdb_page_get(mc->mc_txn, pg, &omp, &level)) != 0)
				return rc2;
			ovpages = omp->mp_pages;

			// Is the ov page large enough? 
			if (ovpages >= dpages) {
			  if (!(omp->mp_flags & P_DIRTY) &&
				  (level || (env->me_flags & MDB_WRITEMAP)))
			  {
				rc = mdb_page_unspill(mc->mc_txn, omp, &omp);
				if (rc)
					return rc;
				level = 0;		// dirty in this txn or clean 
			  }
			  // Is it dirty?
			  if (omp->mp_flags & P_DIRTY) {
				// yes, overwrite it. Note in this case we don't
				// bother to try shrinking the page if the new data
				// is smaller than the overflow threshold.
				if (level > 1) {
					// It is writable only in a parent txn
					size_t sz = (size_t) env->me_psize * ovpages, off;
					MDB_page *np = mdb_page_malloc(mc->mc_txn, ovpages);
					MDB_ID2 id2;
					if (!np)
						return ENOMEM;
					id2.mid = pg;
					id2.mptr = np;
					rc = mdb_mid2l_insert(mc->mc_txn->mt_u.dirty_list, &id2);
					mdb_cassert(mc, rc == 0);
					if (!(flags & MDB_RESERVE)) {
						// Copy end of page, adjusting alignment so
						// compiler may copy words instead of bytes.
						off = (PAGEHDRSZ + data->mv_size) & -sizeof(size_t);
						memcpy((size_t *)((char *)np + off),
							(size_t *)((char *)omp + off), sz - off);
						sz = PAGEHDRSZ;
					}
					memcpy(np, omp, sz); // Copy beginning of page
					omp = np;
				}
				SETDSZ(leaf, data->mv_size);
				if (F_ISSET(flags, MDB_RESERVE))
					data->mv_data = METADATA(omp);
				else
					memcpy(METADATA(omp), data->mv_data, data->mv_size);
				goto done;
			  }
			}
			if ((rc2 = mdb_ovpage_free(mc, omp)) != MDB_SUCCESS)
				return rc2;
		} else if (data->mv_size == olddata.mv_size) {
			// same size, just replace it. Note that we could
			// also reuse this node if the new data is smaller,
			// but instead we opt to shrink the node in that case.
			if (F_ISSET(flags, MDB_RESERVE))
				data->mv_data = olddata.mv_data;
			else if (data->mv_size)
				memcpy(olddata.mv_data, data->mv_data, data->mv_size);
			else
				memcpy(NODEKEY(leaf), key->mv_data, key->mv_size);
			goto done;
		}
		mdb_node_del(mc, 0);
		mc->mc_db->md_entries--;
	}

	rdata = data;

new_sub:
	nflags = flags & NODE_ADD_FLAGS;
	nsize = IS_LEAF2(mc->mc_pg[mc->mc_top]) ? key->mv_size : mdb_leaf_size(env, key, rdata);
	if (SIZELEFT(mc->mc_pg[mc->mc_top]) < nsize) {
		if (( flags & (F_DUPDATA|F_SUBDATA)) == F_DUPDATA )
			nflags &= ~MDB_APPEND;
		if (!insert)
			nflags |= MDB_SPLIT_REPLACE;
		rc = mdb_page_split(mc, key, rdata, P_INVALID, nflags);
	} else {
		// There is room already in this leaf page.
		rc = mdb_node_add(mc, mc->mc_ki[mc->mc_top], key, rdata, 0, nflags);
		if (rc == 0 && !do_sub && insert) {
			// Adjust other cursors pointing to mp
			MDB_cursor *m2, *m3;
			MDB_dbi dbi = mc->mc_dbi;
			unsigned i = mc->mc_top;
			MDB_page *mp = mc->mc_pg[i];

			for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
				if (mc->mc_flags & C_SUB)
					m3 = &m2->mc_xcursor->mx_cursor;
				else
					m3 = m2;
				if (m3 == mc || m3->mc_snum < mc->mc_snum) continue;
				if (m3->mc_pg[i] == mp && m3->mc_ki[i] >= mc->mc_ki[i]) {
					m3->mc_ki[i]++;
				}
			}
		}
	}

	if (rc != MDB_SUCCESS)
		mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
	else {
		// Now store the actual data in the child DB. Note that we're
		// storing the user data in the keys field, so there are strict
		// size limits on dupdata. The actual data fields of the child
		// DB are all zero size.
		if (do_sub) {
			int xflags;
put_sub:
			xdata.mv_size = 0;
			xdata.mv_data = "";
			leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
			if (flags & MDB_CURRENT) {
				xflags = MDB_CURRENT|MDB_NOSPILL;
			} else {
				mdb_xcursor_init1(mc, leaf);
				xflags = (flags & MDB_NODUPDATA) ?
					MDB_NOOVERWRITE|MDB_NOSPILL : MDB_NOSPILL;
			}
			// converted, write the original data first
			if (dkey.mv_size) {
				rc = mdb_cursor_put(&mc->mc_xcursor->mx_cursor, &dkey, &xdata, xflags);
				if (rc)
					return rc;
				{
					// Adjust other cursors pointing to mp
					MDB_cursor *m2;
					unsigned i = mc->mc_top;
					MDB_page *mp = mc->mc_pg[i];

					for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2=m2->mc_next) {
						if (m2 == mc || m2->mc_snum < mc->mc_snum) continue;
						if (!(m2->mc_flags & C_INITIALIZED)) continue;
						if (m2->mc_pg[i] == mp && m2->mc_ki[i] == mc->mc_ki[i]) {
							mdb_xcursor_init1(m2, leaf);
						}
					}
				}
				// we've done our job
				dkey.mv_size = 0;
			}
			if (flags & MDB_APPENDDUP)
				xflags |= MDB_APPEND;
			rc = mdb_cursor_put(&mc->mc_xcursor->mx_cursor, data, &xdata, xflags);
			if (flags & F_SUBDATA) {
				void *db = NODEDATA(leaf);
				memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDB_db));
			}
		}
		// sub-writes might have failed so check rc again.
		// Don't increment count if we just replaced an existing item.
		if (!rc && !(flags & MDB_CURRENT))
			mc->mc_db->md_entries++;
		if (flags & MDB_MULTIPLE) {
			if (!rc) {
next_mult:
				mcount++;
				// let caller know how many succeeded, if any
				data[1].mv_size = mcount;
				if (mcount < dcount) {
					data[0].mv_data = (char *)data[0].mv_data + data[0].mv_size;
					goto more;
				}
			}
		}
	}
done:
	// If we succeeded and the key didn't exist before, make sure
	// the cursor is marked valid.
	if (!rc && insert)
		mc->mc_flags |= C_INITIALIZED;
	return rc;
	*/
	return nil
}

func (c *cursor) Del(flags int) error {
	/*
	MDB_node	*leaf;
	MDB_page	*mp;
	int rc;

	if (mc->mc_txn->mt_flags & (MDB_TXN_RDONLY|MDB_TXN_ERROR))
		return (mc->mc_txn->mt_flags & MDB_TXN_RDONLY) ? EACCES : MDB_BAD_TXN;

	if (!(mc->mc_flags & C_INITIALIZED))
		return EINVAL;

	if (mc->mc_ki[mc->mc_top] >= NUMKEYS(mc->mc_pg[mc->mc_top]))
		return MDB_NOTFOUND;

	if (!(flags & MDB_NOSPILL) && (rc = mdb_page_spill(mc, NULL, NULL)))
		return rc;

	rc = mdb_cursor_touch(mc);
	if (rc)
		return rc;

	mp = mc->mc_pg[mc->mc_top];
	leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

	if (!IS_LEAF2(mp) && F_ISSET(leaf->mn_flags, F_DUPDATA)) {
		if (!(flags & MDB_NODUPDATA)) {
			if (!F_ISSET(leaf->mn_flags, F_SUBDATA)) {
				mc->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
			}
			rc = mdb_cursor_del(&mc->mc_xcursor->mx_cursor, MDB_NOSPILL);
			// If sub-DB still has entries, we're done
			if (mc->mc_xcursor->mx_db.md_entries) {
				if (leaf->mn_flags & F_SUBDATA) {
					// update subDB info
					void *db = NODEDATA(leaf);
					memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDB_db));
				} else {
					MDB_cursor *m2;
					// shrink fake page
					mdb_node_shrink(mp, mc->mc_ki[mc->mc_top]);
					leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
					mc->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
					// fix other sub-DB cursors pointed at this fake page
					for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2=m2->mc_next) {
						if (m2 == mc || m2->mc_snum < mc->mc_snum) continue;
						if (m2->mc_pg[mc->mc_top] == mp &&
							m2->mc_ki[mc->mc_top] == mc->mc_ki[mc->mc_top])
							m2->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
					}
				}
				mc->mc_db->md_entries--;
				mc->mc_flags |= C_DEL;
				return rc;
			}
			// otherwise fall thru and delete the sub-DB
		}

		if (leaf->mn_flags & F_SUBDATA) {
			// add all the child DB's pages to the free list
			rc = mdb_drop0(&mc->mc_xcursor->mx_cursor, 0);
			if (rc == MDB_SUCCESS) {
				mc->mc_db->md_entries -=
					mc->mc_xcursor->mx_db.md_entries;
			}
		}
	}

	return mdb_cursor_del0(mc, leaf);
	*/
	return nil
}

// Allocate and initialize new pages for a database.
// @param[in] mc a cursor on the database being added to.
// @param[in] flags flags defining what type of page is being allocated.
// @param[in] num the number of pages to allocate. This is usually 1,
// unless allocating overflow pages for a large record.
// @param[out] mp Address of a page, or NULL on failure.
// @return 0 on success, non-zero on failure.
func (c *cursor) newPage(flags int, num int) ([]*page, error) {
	/*
	MDB_page	*np;
	int rc;

	if ((rc = mdb_page_alloc(mc, num, &np)))
		return rc;
	DPRINTF(("allocated new mpage %"Z"u, page size %u",
	    np->mp_pgno, mc->mc_txn->mt_env->me_psize));
	np->mp_flags = flags | P_DIRTY;
	np->mp_lower = PAGEHDRSZ;
	np->mp_upper = mc->mc_txn->mt_env->me_psize;

	if (IS_BRANCH(np))
		mc->mc_db->md_branch_pages++;
	else if (IS_LEAF(np))
		mc->mc_db->md_leaf_pages++;
	else if (IS_OVERFLOW(np)) {
		mc->mc_db->md_overflow_pages += num;
		np->mp_pages = num;
	}
	*mp = np;

	return 0;
	*/
	return nil, nil
}

// Add a node to the page pointed to by the cursor.
// @param[in] mc The cursor for this operation.
// @param[in] indx The index on the page where the new node should be added.
// @param[in] key The key for the new node.
// @param[in] data The data for the new node, if any.
// @param[in] pgno The page number, if adding a branch node.
// @param[in] flags Flags for the node.
// @return 0 on success, non-zero on failure. Possible errors are:
// <ul>
//	<li>ENOMEM - failed to allocate overflow pages for the node.
//	<li>MDB_PAGE_FULL - there is insufficient room in the page. This error
//	should never happen since all callers already calculate the
//	page's free space before calling this function.
// </ul>
func (c *cursor) addNode(index int, key []byte, data []byte, pgno int, flags int) error {
	/*
	unsigned int	 i;
	size_t		 node_size = NODESIZE;
	ssize_t		 room;
	indx_t		 ofs;
	MDB_node	*node;
	MDB_page	*mp = mc->mc_pg[mc->mc_top];
	MDB_page	*ofp = NULL;		// overflow page
	DKBUF;

	mdb_cassert(mc, mp->mp_upper >= mp->mp_lower);

	DPRINTF(("add to %s %spage %"Z"u index %i, data size %"Z"u key size %"Z"u [%s]",
	    IS_LEAF(mp) ? "leaf" : "branch",
		IS_SUBP(mp) ? "sub-" : "",
		mdb_dbg_pgno(mp), indx, data ? data->mv_size : 0,
		key ? key->mv_size : 0, key ? DKEY(key) : "null"));

	if (IS_LEAF2(mp)) {
		// Move higher keys up one slot.
		int ksize = mc->mc_db->md_pad, dif;
		char *ptr = LEAF2KEY(mp, indx, ksize);
		dif = NUMKEYS(mp) - indx;
		if (dif > 0)
			memmove(ptr+ksize, ptr, dif*ksize);
		// insert new key
		memcpy(ptr, key->mv_data, ksize);

		// Just using these for counting
		mp->mp_lower += sizeof(indx_t);
		mp->mp_upper -= ksize - sizeof(indx_t);
		return MDB_SUCCESS;
	}

	room = (ssize_t)SIZELEFT(mp) - (ssize_t)sizeof(indx_t);
	if (key != NULL)
		node_size += key->mv_size;
	if (IS_LEAF(mp)) {
		mdb_cassert(mc, data);
		if (F_ISSET(flags, F_BIGDATA)) {
			// Data already on overflow page.
			node_size += sizeof(pgno_t);
		} else if (node_size + data->mv_size > mc->mc_txn->mt_env->me_nodemax) {
			int ovpages = OVPAGES(data->mv_size, mc->mc_txn->mt_env->me_psize);
			int rc;
			// Put data on overflow page.
			DPRINTF(("data size is %"Z"u, node would be %"Z"u, put data on overflow page",
			    data->mv_size, node_size+data->mv_size));
			node_size = EVEN(node_size + sizeof(pgno_t));
			if ((ssize_t)node_size > room)
				goto full;
			if ((rc = mdb_page_new(mc, P_OVERFLOW, ovpages, &ofp)))
				return rc;
			DPRINTF(("allocated overflow page %"Z"u", ofp->mp_pgno));
			flags |= F_BIGDATA;
			goto update;
		} else {
			node_size += data->mv_size;
		}
	}
	node_size = EVEN(node_size);
	if ((ssize_t)node_size > room)
		goto full;

update:
	// Move higher pointers up one slot.
	for (i = NUMKEYS(mp); i > indx; i--)
		mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

	// Adjust free space offsets.
	ofs = mp->mp_upper - node_size;
	mdb_cassert(mc, ofs >= mp->mp_lower + sizeof(indx_t));
	mp->mp_ptrs[indx] = ofs;
	mp->mp_upper = ofs;
	mp->mp_lower += sizeof(indx_t);

	// Write the node data.
	node = NODEPTR(mp, indx);
	node->mn_ksize = (key == NULL) ? 0 : key->mv_size;
	node->mn_flags = flags;
	if (IS_LEAF(mp))
		SETDSZ(node,data->mv_size);
	else
		SETPGNO(node,pgno);

	if (key)
		memcpy(NODEKEY(node), key->mv_data, key->mv_size);

	if (IS_LEAF(mp)) {
		mdb_cassert(mc, key);
		if (ofp == NULL) {
			if (F_ISSET(flags, F_BIGDATA))
				memcpy(node->mn_data + key->mv_size, data->mv_data,
				    sizeof(pgno_t));
			else if (F_ISSET(flags, MDB_RESERVE))
				data->mv_data = node->mn_data + key->mv_size;
			else
				memcpy(node->mn_data + key->mv_size, data->mv_data,
				    data->mv_size);
		} else {
			memcpy(node->mn_data + key->mv_size, &ofp->mp_pgno,
			    sizeof(pgno_t));
			if (F_ISSET(flags, MDB_RESERVE))
				data->mv_data = METADATA(ofp);
			else
				memcpy(METADATA(ofp), data->mv_data, data->mv_size);
		}
	}

	return MDB_SUCCESS;

full:
	DPRINTF(("not enough room in page %"Z"u, got %u ptrs",
		mdb_dbg_pgno(mp), NUMKEYS(mp)));
	DPRINTF(("upper-lower = %u - %u = %"Z"d", mp->mp_upper,mp->mp_lower,room));
	DPRINTF(("node size = %"Z"u", node_size));
	mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
	return MDB_PAGE_FULL;
	*/

	return nil
}

// Delete the specified node from a page.
// @param[in] mp The page to operate on.
// @param[in] indx The index of the node to delete.
// @param[in] ksize The size of a node. Only used if the page is
// part of a #MDB_DUPFIXED database.
func (c *cursor) deleteNode(ksize int) {
	/*
	MDB_page *mp = mc->mc_pg[mc->mc_top];
	indx_t	indx = mc->mc_ki[mc->mc_top];
	unsigned int	 sz;
	indx_t		 i, j, numkeys, ptr;
	MDB_node	*node;
	char		*base;

	DPRINTF(("delete node %u on %s page %"Z"u", indx,
	    IS_LEAF(mp) ? "leaf" : "branch", mdb_dbg_pgno(mp)));
	numkeys = NUMKEYS(mp);
	mdb_cassert(mc, indx < numkeys);

	if (IS_LEAF2(mp)) {
		int x = numkeys - 1 - indx;
		base = LEAF2KEY(mp, indx, ksize);
		if (x)
			memmove(base, base + ksize, x * ksize);
		mp->mp_lower -= sizeof(indx_t);
		mp->mp_upper += ksize - sizeof(indx_t);
		return;
	}

	node = NODEPTR(mp, indx);
	sz = NODESIZE + node->mn_ksize;
	if (IS_LEAF(mp)) {
		if (F_ISSET(node->mn_flags, F_BIGDATA))
			sz += sizeof(pgno_t);
		else
			sz += NODEDSZ(node);
	}
	sz = EVEN(sz);

	ptr = mp->mp_ptrs[indx];
	for (i = j = 0; i < numkeys; i++) {
		if (i != indx) {
			mp->mp_ptrs[j] = mp->mp_ptrs[i];
			if (mp->mp_ptrs[i] < ptr)
				mp->mp_ptrs[j] += sz;
			j++;
		}
	}

	base = (char *)mp + mp->mp_upper;
	memmove(base + sz, base, ptr - mp->mp_upper);

	mp->mp_lower -= sizeof(indx_t);
	mp->mp_upper += sz;
*/
}

// Initial setup of a sorted-dups cursor.
// Sorted duplicates are implemented as a sub-database for the given key.
// The duplicate data items are actually keys of the sub-database.
// Operations on the duplicate data items are performed using a sub-cursor
// initialized when the sub-database is first accessed. This function does
// the preliminary setup of the sub-cursor, filling in the fields that
// depend only on the parent DB.
// @param[in] mc The main cursor whose sorted-dups cursor is to be initialized.
func (c *cursor) xcursor_init0() {
	/*
	MDB_xcursor *mx = mc->mc_xcursor;

	mx->mx_cursor.mc_xcursor = NULL;
	mx->mx_cursor.mc_txn = mc->mc_txn;
	mx->mx_cursor.mc_db = &mx->mx_db;
	mx->mx_cursor.mc_dbx = &mx->mx_dbx;
	mx->mx_cursor.mc_dbi = mc->mc_dbi;
	mx->mx_cursor.mc_dbflag = &mx->mx_dbflag;
	mx->mx_cursor.mc_snum = 0;
	mx->mx_cursor.mc_top = 0;
	mx->mx_cursor.mc_flags = C_SUB;
	mx->mx_dbx.md_name.mv_size = 0;
	mx->mx_dbx.md_name.mv_data = NULL;
	mx->mx_dbx.md_cmp = mc->mc_dbx->md_dcmp;
	mx->mx_dbx.md_dcmp = NULL;
	mx->mx_dbx.md_rel = mc->mc_dbx->md_rel;
	*/
}

// Final setup of a sorted-dups cursor.
//	Sets up the fields that depend on the data from the main cursor.
// @param[in] mc The main cursor whose sorted-dups cursor is to be initialized.
// @param[in] node The data containing the #MDB_db record for the
// sorted-dup database.
func (c *cursor) xcursor_init1(n *node) {
	/*
	MDB_xcursor *mx = mc->mc_xcursor;

	if (node->mn_flags & F_SUBDATA) {
		memcpy(&mx->mx_db, NODEDATA(node), sizeof(MDB_db));
		mx->mx_cursor.mc_pg[0] = 0;
		mx->mx_cursor.mc_snum = 0;
		mx->mx_cursor.mc_top = 0;
		mx->mx_cursor.mc_flags = C_SUB;
	} else {
		MDB_page *fp = NODEDATA(node);
		mx->mx_db.md_pad = mc->mc_pg[mc->mc_top]->mp_pad;
		mx->mx_db.md_flags = 0;
		mx->mx_db.md_depth = 1;
		mx->mx_db.md_branch_pages = 0;
		mx->mx_db.md_leaf_pages = 1;
		mx->mx_db.md_overflow_pages = 0;
		mx->mx_db.md_entries = NUMKEYS(fp);
		COPY_PGNO(mx->mx_db.md_root, fp->mp_pgno);
		mx->mx_cursor.mc_snum = 1;
		mx->mx_cursor.mc_top = 0;
		mx->mx_cursor.mc_flags = C_INITIALIZED|C_SUB;
		mx->mx_cursor.mc_pg[0] = fp;
		mx->mx_cursor.mc_ki[0] = 0;
		if (mc->mc_db->md_flags & MDB_DUPFIXED) {
			mx->mx_db.md_flags = MDB_DUPFIXED;
			mx->mx_db.md_pad = fp->mp_pad;
			if (mc->mc_db->md_flags & MDB_INTEGERDUP)
				mx->mx_db.md_flags |= MDB_INTEGERKEY;
		}
	}
	DPRINTF(("Sub-db -%u root page %"Z"u", mx->mx_cursor.mc_dbi,
		mx->mx_db.md_root));
	mx->mx_dbflag = DB_VALID|DB_DIRTY; // DB_DIRTY guides mdb_cursor_touch
#if UINT_MAX < SIZE_MAX
	if (mx->mx_dbx.md_cmp == mdb_cmp_int && mx->mx_db.md_pad == sizeof(size_t))
#ifdef MISALIGNED_OK
		mx->mx_dbx.md_cmp = mdb_cmp_long;
#else
		mx->mx_dbx.md_cmp = mdb_cmp_cint;
#endif
#endif
	*/
}

// Initialize a cursor for a given transaction and database.
func (c *cursor) init(t *transaction, bucket *bucket, mx *xcursor) {
	/*
	mc->mc_next = NULL;
	mc->mc_backup = NULL;
	mc->mc_dbi = dbi;
	mc->mc_txn = txn;
	mc->mc_db = &txn->mt_dbs[dbi];
	mc->mc_dbx = &txn->mt_dbxs[dbi];
	mc->mc_dbflag = &txn->mt_dbflags[dbi];
	mc->mc_snum = 0;
	mc->mc_top = 0;
	mc->mc_pg[0] = 0;
	mc->mc_flags = 0;
	if (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT) {
		mdb_tassert(txn, mx != NULL);
		mc->mc_xcursor = mx;
		mdb_xcursor_init0(mc);
	} else {
		mc->mc_xcursor = NULL;
	}
	if (*mc->mc_dbflag & DB_STALE) {
		mdb_page_search(mc, NULL, MDB_PS_ROOTONLY);
	}
	*/
}

// Return the count of duplicate data items for the current key.
func (c *cursor) count() (int, error) {
	/*
	MDB_node	*leaf;

	if (mc == NULL || countp == NULL)
		return EINVAL;

	if (mc->mc_xcursor == NULL)
		return MDB_INCOMPATIBLE;

	leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
	if (!F_ISSET(leaf->mn_flags, F_DUPDATA)) {
		*countp = 1;
	} else {
		if (!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))
			return EINVAL;

		*countp = mc->mc_xcursor->mx_db.md_entries;
	}
	return MDB_SUCCESS;
	*/
	return 0, nil
}

func (c *cursor) Close() {
	/*
	if (mc && !mc->mc_backup) {
		// remove from txn, if tracked
		if ((mc->mc_flags & C_UNTRACK) && mc->mc_txn->mt_cursors) {
			MDB_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
			while (*prev && *prev != mc) prev = &(*prev)->mc_next;
			if (*prev == mc)
				*prev = mc->mc_next;
		}
		free(mc);
	}
	*/
}

func (c *cursor) Transaction() Transaction {
	/*
	if (!mc) return NULL;
	return mc->mc_txn;
	*/
	return nil
}

func (c *cursor) Bucket() Bucket {
	return c.bucket
}

// Replace the key for a branch node with a new key.
// @param[in] mc Cursor pointing to the node to operate on.
// @param[in] key The new key to use.
// @return 0 on success, non-zero on failure.
func (c *cursor) updateKey(key []byte) error {
	/*
	MDB_page		*mp;
	MDB_node		*node;
	char			*base;
	size_t			 len;
	int				 delta, ksize, oksize;
	indx_t			 ptr, i, numkeys, indx;
	DKBUF;

	indx = mc->mc_ki[mc->mc_top];
	mp = mc->mc_pg[mc->mc_top];
	node = NODEPTR(mp, indx);
	ptr = mp->mp_ptrs[indx];
#if MDB_DEBUG
	{
		MDB_val	k2;
		char kbuf2[DKBUF_MAXKEYSIZE*2+1];
		k2.mv_data = NODEKEY(node);
		k2.mv_size = node->mn_ksize;
		DPRINTF(("update key %u (ofs %u) [%s] to [%s] on page %"Z"u",
			indx, ptr,
			mdb_dkey(&k2, kbuf2),
			DKEY(key),
			mp->mp_pgno));
	}
#endif

	// Sizes must be 2-byte aligned.
	ksize = EVEN(key->mv_size);
	oksize = EVEN(node->mn_ksize);
	delta = ksize - oksize;

	// Shift node contents if EVEN(key length) changed.
	if (delta) {
		if (delta > 0 && SIZELEFT(mp) < delta) {
			pgno_t pgno;
			// not enough space left, do a delete and split
			DPRINTF(("Not enough room, delta = %d, splitting...", delta));
			pgno = NODEPGNO(node);
			mdb_node_del(mc, 0);
			return mdb_page_split(mc, key, NULL, pgno, MDB_SPLIT_REPLACE);
		}

		numkeys = NUMKEYS(mp);
		for (i = 0; i < numkeys; i++) {
			if (mp->mp_ptrs[i] <= ptr)
				mp->mp_ptrs[i] -= delta;
		}

		base = (char *)mp + mp->mp_upper;
		len = ptr - mp->mp_upper + NODESIZE;
		memmove(base - delta, base, len);
		mp->mp_upper -= delta;

		node = NODEPTR(mp, indx);
	}

	// But even if no shift was needed, update ksize
	if (node->mn_ksize != key->mv_size)
		node->mn_ksize = key->mv_size;

	if (key->mv_size)
		memcpy(NODEKEY(node), key->mv_data, key->mv_size);

	return MDB_SUCCESS;
	*/
	return nil
}

// Move a node from csrc to cdst.
func (c *cursor) moveNodeTo(dst *cursor) error {
	/*
	MDB_node		*srcnode;
	MDB_val		 key, data;
	pgno_t	srcpg;
	MDB_cursor mn;
	int			 rc;
	unsigned short flags;

	DKBUF;

	// Mark src and dst as dirty.
	if ((rc = mdb_page_touch(csrc)) ||
	    (rc = mdb_page_touch(cdst)))
		return rc;

	if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
		key.mv_size = csrc->mc_db->md_pad;
		key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top], key.mv_size);
		data.mv_size = 0;
		data.mv_data = NULL;
		srcpg = 0;
		flags = 0;
	} else {
		srcnode = NODEPTR(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top]);
		mdb_cassert(csrc, !((size_t)srcnode & 1));
		srcpg = NODEPGNO(srcnode);
		flags = srcnode->mn_flags;
		if (csrc->mc_ki[csrc->mc_top] == 0 && IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
			unsigned int snum = csrc->mc_snum;
			MDB_node *s2;
			// must find the lowest key below src
			mdb_page_search_lowest(csrc);
			if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
				key.mv_size = csrc->mc_db->md_pad;
				key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], 0, key.mv_size);
			} else {
				s2 = NODEPTR(csrc->mc_pg[csrc->mc_top], 0);
				key.mv_size = NODEKSZ(s2);
				key.mv_data = NODEKEY(s2);
			}
			csrc->mc_snum = snum--;
			csrc->mc_top = snum;
		} else {
			key.mv_size = NODEKSZ(srcnode);
			key.mv_data = NODEKEY(srcnode);
		}
		data.mv_size = NODEDSZ(srcnode);
		data.mv_data = NODEDATA(srcnode);
	}
	if (IS_BRANCH(cdst->mc_pg[cdst->mc_top]) && cdst->mc_ki[cdst->mc_top] == 0) {
		unsigned int snum = cdst->mc_snum;
		MDB_node *s2;
		MDB_val bkey;
		// must find the lowest key below dst
		mdb_page_search_lowest(cdst);
		if (IS_LEAF2(cdst->mc_pg[cdst->mc_top])) {
			bkey.mv_size = cdst->mc_db->md_pad;
			bkey.mv_data = LEAF2KEY(cdst->mc_pg[cdst->mc_top], 0, bkey.mv_size);
		} else {
			s2 = NODEPTR(cdst->mc_pg[cdst->mc_top], 0);
			bkey.mv_size = NODEKSZ(s2);
			bkey.mv_data = NODEKEY(s2);
		}
		cdst->mc_snum = snum--;
		cdst->mc_top = snum;
		mdb_cursor_copy(cdst, &mn);
		mn.mc_ki[snum] = 0;
		rc = mdb_update_key(&mn, &bkey);
		if (rc)
			return rc;
	}

	DPRINTF(("moving %s node %u [%s] on page %"Z"u to node %u on page %"Z"u",
	    IS_LEAF(csrc->mc_pg[csrc->mc_top]) ? "leaf" : "branch",
	    csrc->mc_ki[csrc->mc_top],
		DKEY(&key),
	    csrc->mc_pg[csrc->mc_top]->mp_pgno,
	    cdst->mc_ki[cdst->mc_top], cdst->mc_pg[cdst->mc_top]->mp_pgno));

	// Add the node to the destination page.
	rc = mdb_node_add(cdst, cdst->mc_ki[cdst->mc_top], &key, &data, srcpg, flags);
	if (rc != MDB_SUCCESS)
		return rc;

	// Delete the node from the source page.
	mdb_node_del(csrc, key.mv_size);

	{
		// Adjust other cursors pointing to mp
		MDB_cursor *m2, *m3;
		MDB_dbi dbi = csrc->mc_dbi;
		MDB_page *mp = csrc->mc_pg[csrc->mc_top];

		for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
			if (csrc->mc_flags & C_SUB)
				m3 = &m2->mc_xcursor->mx_cursor;
			else
				m3 = m2;
			if (m3 == csrc) continue;
			if (m3->mc_pg[csrc->mc_top] == mp && m3->mc_ki[csrc->mc_top] ==
				csrc->mc_ki[csrc->mc_top]) {
				m3->mc_pg[csrc->mc_top] = cdst->mc_pg[cdst->mc_top];
				m3->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
			}
		}
	}

	// Update the parent separators.
	if (csrc->mc_ki[csrc->mc_top] == 0) {
		if (csrc->mc_ki[csrc->mc_top-1] != 0) {
			if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
				key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], 0, key.mv_size);
			} else {
				srcnode = NODEPTR(csrc->mc_pg[csrc->mc_top], 0);
				key.mv_size = NODEKSZ(srcnode);
				key.mv_data = NODEKEY(srcnode);
			}
			DPRINTF(("update separator for source page %"Z"u to [%s]",
				csrc->mc_pg[csrc->mc_top]->mp_pgno, DKEY(&key)));
			mdb_cursor_copy(csrc, &mn);
			mn.mc_snum--;
			mn.mc_top--;
			if ((rc = mdb_update_key(&mn, &key)) != MDB_SUCCESS)
				return rc;
		}
		if (IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
			MDB_val	 nullkey;
			indx_t	ix = csrc->mc_ki[csrc->mc_top];
			nullkey.mv_size = 0;
			csrc->mc_ki[csrc->mc_top] = 0;
			rc = mdb_update_key(csrc, &nullkey);
			csrc->mc_ki[csrc->mc_top] = ix;
			mdb_cassert(csrc, rc == MDB_SUCCESS);
		}
	}

	if (cdst->mc_ki[cdst->mc_top] == 0) {
		if (cdst->mc_ki[cdst->mc_top-1] != 0) {
			if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
				key.mv_data = LEAF2KEY(cdst->mc_pg[cdst->mc_top], 0, key.mv_size);
			} else {
				srcnode = NODEPTR(cdst->mc_pg[cdst->mc_top], 0);
				key.mv_size = NODEKSZ(srcnode);
				key.mv_data = NODEKEY(srcnode);
			}
			DPRINTF(("update separator for destination page %"Z"u to [%s]",
				cdst->mc_pg[cdst->mc_top]->mp_pgno, DKEY(&key)));
			mdb_cursor_copy(cdst, &mn);
			mn.mc_snum--;
			mn.mc_top--;
			if ((rc = mdb_update_key(&mn, &key)) != MDB_SUCCESS)
				return rc;
		}
		if (IS_BRANCH(cdst->mc_pg[cdst->mc_top])) {
			MDB_val	 nullkey;
			indx_t	ix = cdst->mc_ki[cdst->mc_top];
			nullkey.mv_size = 0;
			cdst->mc_ki[cdst->mc_top] = 0;
			rc = mdb_update_key(cdst, &nullkey);
			cdst->mc_ki[cdst->mc_top] = ix;
			mdb_cassert(csrc, rc == MDB_SUCCESS);
		}
	}

	return MDB_SUCCESS;
	*/

	return nil
}

// Merge one page into another.
//  The nodes from the page pointed to by \b csrc will
//	be copied to the page pointed to by \b cdst and then
//	the \b csrc page will be freed.
// @param[in] csrc Cursor pointing to the source page.
// @param[in] cdst Cursor pointing to the destination page.
func (c *cursor) mergePage(dst *cursor) error {
	/*
	int			 rc;
	indx_t			 i, j;
	MDB_node		*srcnode;
	MDB_val		 key, data;
	unsigned	nkeys;

	DPRINTF(("merging page %"Z"u into %"Z"u", csrc->mc_pg[csrc->mc_top]->mp_pgno,
		cdst->mc_pg[cdst->mc_top]->mp_pgno));

	mdb_cassert(csrc, csrc->mc_snum > 1);	// can't merge root page
	mdb_cassert(csrc, cdst->mc_snum > 1);

	// Mark dst as dirty.
	if ((rc = mdb_page_touch(cdst)))
		return rc;

	// Move all nodes from src to dst.
	j = nkeys = NUMKEYS(cdst->mc_pg[cdst->mc_top]);
	if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
		key.mv_size = csrc->mc_db->md_pad;
		key.mv_data = METADATA(csrc->mc_pg[csrc->mc_top]);
		for (i = 0; i < NUMKEYS(csrc->mc_pg[csrc->mc_top]); i++, j++) {
			rc = mdb_node_add(cdst, j, &key, NULL, 0, 0);
			if (rc != MDB_SUCCESS)
				return rc;
			key.mv_data = (char *)key.mv_data + key.mv_size;
		}
	} else {
		for (i = 0; i < NUMKEYS(csrc->mc_pg[csrc->mc_top]); i++, j++) {
			srcnode = NODEPTR(csrc->mc_pg[csrc->mc_top], i);
			if (i == 0 && IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
				unsigned int snum = csrc->mc_snum;
				MDB_node *s2;
				// must find the lowest key below src
				mdb_page_search_lowest(csrc);
				if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
					key.mv_size = csrc->mc_db->md_pad;
					key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], 0, key.mv_size);
				} else {
					s2 = NODEPTR(csrc->mc_pg[csrc->mc_top], 0);
					key.mv_size = NODEKSZ(s2);
					key.mv_data = NODEKEY(s2);
				}
				csrc->mc_snum = snum--;
				csrc->mc_top = snum;
			} else {
				key.mv_size = srcnode->mn_ksize;
				key.mv_data = NODEKEY(srcnode);
			}

			data.mv_size = NODEDSZ(srcnode);
			data.mv_data = NODEDATA(srcnode);
			rc = mdb_node_add(cdst, j, &key, &data, NODEPGNO(srcnode), srcnode->mn_flags);
			if (rc != MDB_SUCCESS)
				return rc;
		}
	}

	DPRINTF(("dst page %"Z"u now has %u keys (%.1f%% filled)",
	    cdst->mc_pg[cdst->mc_top]->mp_pgno, NUMKEYS(cdst->mc_pg[cdst->mc_top]),
		(float)PAGEFILL(cdst->mc_txn->mt_env, cdst->mc_pg[cdst->mc_top]) / 10));

	// Unlink the src page from parent and add to free list.
	csrc->mc_top--;
	mdb_node_del(csrc, 0);
	if (csrc->mc_ki[csrc->mc_top] == 0) {
		key.mv_size = 0;
		rc = mdb_update_key(csrc, &key);
		if (rc) {
			csrc->mc_top++;
			return rc;
		}
	}
	csrc->mc_top++;

	rc = mdb_midl_append(&csrc->mc_txn->mt_free_pgs,
		csrc->mc_pg[csrc->mc_top]->mp_pgno);
	if (rc)
		return rc;
	if (IS_LEAF(csrc->mc_pg[csrc->mc_top]))
		csrc->mc_db->md_leaf_pages--;
	else
		csrc->mc_db->md_branch_pages--;
	{
		// Adjust other cursors pointing to mp 
		MDB_cursor *m2, *m3;
		MDB_dbi dbi = csrc->mc_dbi;
		MDB_page *mp = cdst->mc_pg[cdst->mc_top];

		for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
			if (csrc->mc_flags & C_SUB)
				m3 = &m2->mc_xcursor->mx_cursor;
			else
				m3 = m2;
			if (m3 == csrc) continue;
			if (m3->mc_snum < csrc->mc_snum) continue;
			if (m3->mc_pg[csrc->mc_top] == csrc->mc_pg[csrc->mc_top]) {
				m3->mc_pg[csrc->mc_top] = mp;
				m3->mc_ki[csrc->mc_top] += nkeys;
			}
		}
	}
	mdb_cursor_pop(csrc);

	return mdb_rebalance(csrc);
	*/

	return nil
}

// Copy the contents of a cursor.
// @param[in] csrc The cursor to copy from.
// @param[out] cdst The cursor to copy to.
func (c *cursor) copyTo(dst *cursor) {
	/*
	unsigned int i;

	cdst->mc_txn = csrc->mc_txn;
	cdst->mc_dbi = csrc->mc_dbi;
	cdst->mc_db  = csrc->mc_db;
	cdst->mc_dbx = csrc->mc_dbx;
	cdst->mc_snum = csrc->mc_snum;
	cdst->mc_top = csrc->mc_top;
	cdst->mc_flags = csrc->mc_flags;

	for (i=0; i<csrc->mc_snum; i++) {
		cdst->mc_pg[i] = csrc->mc_pg[i];
		cdst->mc_ki[i] = csrc->mc_ki[i];
	}
	*/
}

// Rebalance the tree after a delete operation.
// @param[in] mc Cursor pointing to the page where rebalancing
// should begin.
// @return 0 on success, non-zero on failure.
func (c *cursor) rebalance() error {
	/*
	MDB_node	*node;
	int rc;
	unsigned int ptop, minkeys;
	MDB_cursor	mn;

	minkeys = 1 + (IS_BRANCH(mc->mc_pg[mc->mc_top]));
	DPRINTF(("rebalancing %s page %"Z"u (has %u keys, %.1f%% full)",
	    IS_LEAF(mc->mc_pg[mc->mc_top]) ? "leaf" : "branch",
	    mdb_dbg_pgno(mc->mc_pg[mc->mc_top]), NUMKEYS(mc->mc_pg[mc->mc_top]),
		(float)PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) / 10));

	if (PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) >= FILL_THRESHOLD &&
		NUMKEYS(mc->mc_pg[mc->mc_top]) >= minkeys) {
		DPRINTF(("no need to rebalance page %"Z"u, above fill threshold",
		    mdb_dbg_pgno(mc->mc_pg[mc->mc_top])));
		return MDB_SUCCESS;
	}

	if (mc->mc_snum < 2) {
		MDB_page *mp = mc->mc_pg[0];
		if (IS_SUBP(mp)) {
			DPUTS("Can't rebalance a subpage, ignoring");
			return MDB_SUCCESS;
		}
		if (NUMKEYS(mp) == 0) {
			DPUTS("tree is completely empty");
			mc->mc_db->md_root = P_INVALID;
			mc->mc_db->md_depth = 0;
			mc->mc_db->md_leaf_pages = 0;
			rc = mdb_midl_append(&mc->mc_txn->mt_free_pgs, mp->mp_pgno);
			if (rc)
				return rc;
			// Adjust cursors pointing to mp
			mc->mc_snum = 0;
			mc->mc_top = 0;
			mc->mc_flags &= ~C_INITIALIZED;
			{
				MDB_cursor *m2, *m3;
				MDB_dbi dbi = mc->mc_dbi;

				for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
					if (mc->mc_flags & C_SUB)
						m3 = &m2->mc_xcursor->mx_cursor;
					else
						m3 = m2;
					if (m3->mc_snum < mc->mc_snum) continue;
					if (m3->mc_pg[0] == mp) {
						m3->mc_snum = 0;
						m3->mc_top = 0;
						m3->mc_flags &= ~C_INITIALIZED;
					}
				}
			}
		} else if (IS_BRANCH(mp) && NUMKEYS(mp) == 1) {
			DPUTS("collapsing root page!");
			rc = mdb_midl_append(&mc->mc_txn->mt_free_pgs, mp->mp_pgno);
			if (rc)
				return rc;
			mc->mc_db->md_root = NODEPGNO(NODEPTR(mp, 0));
			rc = mdb_page_get(mc->mc_txn,mc->mc_db->md_root,&mc->mc_pg[0],NULL);
			if (rc)
				return rc;
			mc->mc_db->md_depth--;
			mc->mc_db->md_branch_pages--;
			mc->mc_ki[0] = mc->mc_ki[1];
			{
				// Adjust other cursors pointing to mp
				MDB_cursor *m2, *m3;
				MDB_dbi dbi = mc->mc_dbi;

				for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
					if (mc->mc_flags & C_SUB)
						m3 = &m2->mc_xcursor->mx_cursor;
					else
						m3 = m2;
					if (m3 == mc || m3->mc_snum < mc->mc_snum) continue;
					if (m3->mc_pg[0] == mp) {
						int i;
						m3->mc_snum--;
						m3->mc_top--;
						for (i=0; i<m3->mc_snum; i++) {
							m3->mc_pg[i] = m3->mc_pg[i+1];
							m3->mc_ki[i] = m3->mc_ki[i+1];
						}
					}
				}
			}
		} else
			DPUTS("root page doesn't need rebalancing");
		return MDB_SUCCESS;
	}

	// The parent (branch page) must have at least 2 pointers,
	// otherwise the tree is invalid.
	ptop = mc->mc_top-1;
	mdb_cassert(mc, NUMKEYS(mc->mc_pg[ptop]) > 1);

	// Leaf page fill factor is below the threshold.
	// Try to move keys from left or right neighbor, or
	// merge with a neighbor page.

	// Find neighbors.
	mdb_cursor_copy(mc, &mn);
	mn.mc_xcursor = NULL;

	if (mc->mc_ki[ptop] == 0) {
		// We're the leftmost leaf in our parent.
		DPUTS("reading right neighbor");
		mn.mc_ki[ptop]++;
		node = NODEPTR(mc->mc_pg[ptop], mn.mc_ki[ptop]);
		rc = mdb_page_get(mc->mc_txn,NODEPGNO(node),&mn.mc_pg[mn.mc_top],NULL);
		if (rc)
			return rc;
		mn.mc_ki[mn.mc_top] = 0;
		mc->mc_ki[mc->mc_top] = NUMKEYS(mc->mc_pg[mc->mc_top]);
	} else {
		// There is at least one neighbor to the left.
		DPUTS("reading left neighbor");
		mn.mc_ki[ptop]--;
		node = NODEPTR(mc->mc_pg[ptop], mn.mc_ki[ptop]);
		rc = mdb_page_get(mc->mc_txn,NODEPGNO(node),&mn.mc_pg[mn.mc_top],NULL);
		if (rc)
			return rc;
		mn.mc_ki[mn.mc_top] = NUMKEYS(mn.mc_pg[mn.mc_top]) - 1;
		mc->mc_ki[mc->mc_top] = 0;
	}

	DPRINTF(("found neighbor page %"Z"u (%u keys, %.1f%% full)",
	    mn.mc_pg[mn.mc_top]->mp_pgno, NUMKEYS(mn.mc_pg[mn.mc_top]),
		(float)PAGEFILL(mc->mc_txn->mt_env, mn.mc_pg[mn.mc_top]) / 10));

	// If the neighbor page is above threshold and has enough keys,
	// move one key from it. Otherwise we should try to merge them.
	// (A branch page must never have less than 2 keys.)
	minkeys = 1 + (IS_BRANCH(mn.mc_pg[mn.mc_top]));
	if (PAGEFILL(mc->mc_txn->mt_env, mn.mc_pg[mn.mc_top]) >= FILL_THRESHOLD && NUMKEYS(mn.mc_pg[mn.mc_top]) > minkeys)
		return mdb_node_move(&mn, mc);
	else {
		if (mc->mc_ki[ptop] == 0)
			rc = mdb_page_merge(&mn, mc);
		else {
			mn.mc_ki[mn.mc_top] += mc->mc_ki[mn.mc_top] + 1;
			rc = mdb_page_merge(mc, &mn);
			mdb_cursor_copy(&mn, mc);
		}
		mc->mc_flags &= ~(C_INITIALIZED|C_EOF);
	}
	return rc;
	*/
	return nil
}

// Complete a delete operation started by #mdb_cursor_del().
func (c *cursor) del0(leaf *node) error {
	/*
	int rc;
	MDB_page *mp;
	indx_t ki;
	unsigned int nkeys;

	mp = mc->mc_pg[mc->mc_top];
	ki = mc->mc_ki[mc->mc_top];

	// add overflow pages to free list
	if (!IS_LEAF2(mp) && F_ISSET(leaf->mn_flags, F_BIGDATA)) {
		MDB_page *omp;
		pgno_t pg;

		memcpy(&pg, NODEDATA(leaf), sizeof(pg));
		if ((rc = mdb_page_get(mc->mc_txn, pg, &omp, NULL)) ||
			(rc = mdb_ovpage_free(mc, omp)))
			return rc;
	}
	mdb_node_del(mc, mc->mc_db->md_pad);
	mc->mc_db->md_entries--;
	rc = mdb_rebalance(mc);
	if (rc != MDB_SUCCESS)
		mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
	else {
		MDB_cursor *m2, *m3;
		MDB_dbi dbi = mc->mc_dbi;

		mp = mc->mc_pg[mc->mc_top];
		nkeys = NUMKEYS(mp);

		// if mc points past last node in page, find next sibling
		if (mc->mc_ki[mc->mc_top] >= nkeys)
			mdb_cursor_sibling(mc, 1);

		// Adjust other cursors pointing to mp
		for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
			m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
			if (! (m2->mc_flags & m3->mc_flags & C_INITIALIZED))
				continue;
			if (m3 == mc || m3->mc_snum < mc->mc_snum)
				continue;
			if (m3->mc_pg[mc->mc_top] == mp) {
				if (m3->mc_ki[mc->mc_top] >= ki) {
					m3->mc_flags |= C_DEL;
					if (m3->mc_ki[mc->mc_top] > ki)
						m3->mc_ki[mc->mc_top]--;
				}
				if (m3->mc_ki[mc->mc_top] >= nkeys)
					mdb_cursor_sibling(m3, 1);
			}
		}
		mc->mc_flags |= C_DEL;
	}

	return rc;
	*/
	return nil
}

// Split a page and insert a new node.
// @param[in,out] mc Cursor pointing to the page and desired insertion index.
// The cursor will be updated to point to the actual page and index where
// the node got inserted after the split.
// @param[in] newkey The key for the newly inserted node.
// @param[in] newdata The data for the newly inserted node.
// @param[in] newpgno The page number, if the new node is a branch node.
// @param[in] nflags The #NODE_ADD_FLAGS for the new node.
// @return 0 on success, non-zero on failure.
func (c *cursor) splitPage(newKey []byte, newData []byte, newpgno int, nflags int) error {
	/*
	unsigned int flags;
	int		 rc = MDB_SUCCESS, new_root = 0, did_split = 0;
	indx_t		 newindx;
	pgno_t		 pgno = 0;
	int	 i, j, split_indx, nkeys, pmax;
	MDB_env 	*env = mc->mc_txn->mt_env;
	MDB_node	*node;
	MDB_val	 sepkey, rkey, xdata, *rdata = &xdata;
	MDB_page	*copy = NULL;
	MDB_page	*mp, *rp, *pp;
	int ptop;
	MDB_cursor	mn;
	DKBUF;

	mp = mc->mc_pg[mc->mc_top];
	newindx = mc->mc_ki[mc->mc_top];
	nkeys = NUMKEYS(mp);

	DPRINTF(("-----> splitting %s page %"Z"u and adding [%s] at index %i/%i",
	    IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno,
	    DKEY(newkey), mc->mc_ki[mc->mc_top], nkeys));

	// Create a right sibling.
	if ((rc = mdb_page_new(mc, mp->mp_flags, 1, &rp)))
		return rc;
	DPRINTF(("new right sibling: page %"Z"u", rp->mp_pgno));

	if (mc->mc_snum < 2) {
		if ((rc = mdb_page_new(mc, P_BRANCH, 1, &pp)))
			return rc;
		// shift current top to make room for new parent
		mc->mc_pg[1] = mc->mc_pg[0];
		mc->mc_ki[1] = mc->mc_ki[0];
		mc->mc_pg[0] = pp;
		mc->mc_ki[0] = 0;
		mc->mc_db->md_root = pp->mp_pgno;
		DPRINTF(("root split! new root = %"Z"u", pp->mp_pgno));
		mc->mc_db->md_depth++;
		new_root = 1;

		// Add left (implicit) pointer.
		if ((rc = mdb_node_add(mc, 0, NULL, NULL, mp->mp_pgno, 0)) != MDB_SUCCESS) {
			// undo the pre-push
			mc->mc_pg[0] = mc->mc_pg[1];
			mc->mc_ki[0] = mc->mc_ki[1];
			mc->mc_db->md_root = mp->mp_pgno;
			mc->mc_db->md_depth--;
			return rc;
		}
		mc->mc_snum = 2;
		mc->mc_top = 1;
		ptop = 0;
	} else {
		ptop = mc->mc_top-1;
		DPRINTF(("parent branch page is %"Z"u", mc->mc_pg[ptop]->mp_pgno));
	}

	mc->mc_flags |= C_SPLITTING;
	mdb_cursor_copy(mc, &mn);
	mn.mc_pg[mn.mc_top] = rp;
	mn.mc_ki[ptop] = mc->mc_ki[ptop]+1;

	if (nflags & MDB_APPEND) {
		mn.mc_ki[mn.mc_top] = 0;
		sepkey = *newkey;
		split_indx = newindx;
		nkeys = 0;
	} else {

		split_indx = (nkeys+1) / 2;

		if (IS_LEAF2(rp)) {
			char *split, *ins;
			int x;
			unsigned int lsize, rsize, ksize;
			// Move half of the keys to the right sibling
			copy = NULL;
			x = mc->mc_ki[mc->mc_top] - split_indx;
			ksize = mc->mc_db->md_pad;
			split = LEAF2KEY(mp, split_indx, ksize);
			rsize = (nkeys - split_indx) * ksize;
			lsize = (nkeys - split_indx) * sizeof(indx_t);
			mp->mp_lower -= lsize;
			rp->mp_lower += lsize;
			mp->mp_upper += rsize - lsize;
			rp->mp_upper -= rsize - lsize;
			sepkey.mv_size = ksize;
			if (newindx == split_indx) {
				sepkey.mv_data = newkey->mv_data;
			} else {
				sepkey.mv_data = split;
			}
			if (x<0) {
				ins = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], ksize);
				memcpy(rp->mp_ptrs, split, rsize);
				sepkey.mv_data = rp->mp_ptrs;
				memmove(ins+ksize, ins, (split_indx - mc->mc_ki[mc->mc_top]) * ksize);
				memcpy(ins, newkey->mv_data, ksize);
				mp->mp_lower += sizeof(indx_t);
				mp->mp_upper -= ksize - sizeof(indx_t);
			} else {
				if (x)
					memcpy(rp->mp_ptrs, split, x * ksize);
				ins = LEAF2KEY(rp, x, ksize);
				memcpy(ins, newkey->mv_data, ksize);
				memcpy(ins+ksize, split + x * ksize, rsize - x * ksize);
				rp->mp_lower += sizeof(indx_t);
				rp->mp_upper -= ksize - sizeof(indx_t);
				mc->mc_ki[mc->mc_top] = x;
				mc->mc_pg[mc->mc_top] = rp;
			}
		} else {
			int psize, nsize, k;
			// Maximum free space in an empty page
			pmax = env->me_psize - PAGEHDRSZ;
			if (IS_LEAF(mp))
				nsize = mdb_leaf_size(env, newkey, newdata);
			else
				nsize = mdb_branch_size(env, newkey);
			nsize = EVEN(nsize);

			// grab a page to hold a temporary copy
			copy = mdb_page_malloc(mc->mc_txn, 1);
			if (copy == NULL)
				return ENOMEM;
			copy->mp_pgno  = mp->mp_pgno;
			copy->mp_flags = mp->mp_flags;
			copy->mp_lower = PAGEHDRSZ;
			copy->mp_upper = env->me_psize;

			// prepare to insert
			for (i=0, j=0; i<nkeys; i++) {
				if (i == newindx) {
					copy->mp_ptrs[j++] = 0;
				}
				copy->mp_ptrs[j++] = mp->mp_ptrs[i];
			}

			// When items are relatively large the split point needs
			// to be checked, because being off-by-one will make the
			// difference between success or failure in mdb_node_add.
			//
			// It's also relevant if a page happens to be laid out
			// such that one half of its nodes are all "small" and
			// the other half of its nodes are "large." If the new
			// item is also "large" and falls on the half with
			// "large" nodes, it also may not fit.
			//
			// As a final tweak, if the new item goes on the last
			// spot on the page (and thus, onto the new page), bias
			// the split so the new page is emptier than the old page.
			// This yields better packing during sequential inserts.
			if (nkeys < 20 || nsize > pmax/16 || newindx >= nkeys) {
				// Find split point
				psize = 0;
				if (newindx <= split_indx || newindx >= nkeys) {
					i = 0; j = 1;
					k = newindx >= nkeys ? nkeys : split_indx+2;
				} else {
					i = nkeys; j = -1;
					k = split_indx-1;
				}
				for (; i!=k; i+=j) {
					if (i == newindx) {
						psize += nsize;
						node = NULL;
					} else {
						node = (MDB_node *)((char *)mp + copy->mp_ptrs[i]);
						psize += NODESIZE + NODEKSZ(node) + sizeof(indx_t);
						if (IS_LEAF(mp)) {
							if (F_ISSET(node->mn_flags, F_BIGDATA))
								psize += sizeof(pgno_t);
							else
								psize += NODEDSZ(node);
						}
						psize = EVEN(psize);
					}
					if (psize > pmax || i == k-j) {
						split_indx = i + (j<0);
						break;
					}
				}
			}
			if (split_indx == newindx) {
				sepkey.mv_size = newkey->mv_size;
				sepkey.mv_data = newkey->mv_data;
			} else {
				node = (MDB_node *)((char *)mp + copy->mp_ptrs[split_indx]);
				sepkey.mv_size = node->mn_ksize;
				sepkey.mv_data = NODEKEY(node);
			}
		}
	}

	DPRINTF(("separator is %d [%s]", split_indx, DKEY(&sepkey)));

	// Copy separator key to the parent.
	if (SIZELEFT(mn.mc_pg[ptop]) < mdb_branch_size(env, &sepkey)) {
		mn.mc_snum--;
		mn.mc_top--;
		did_split = 1;
		rc = mdb_page_split(&mn, &sepkey, NULL, rp->mp_pgno, 0);

		// root split?
		if (mn.mc_snum == mc->mc_snum) {
			mc->mc_pg[mc->mc_snum] = mc->mc_pg[mc->mc_top];
			mc->mc_ki[mc->mc_snum] = mc->mc_ki[mc->mc_top];
			mc->mc_pg[mc->mc_top] = mc->mc_pg[ptop];
			mc->mc_ki[mc->mc_top] = mc->mc_ki[ptop];
			mc->mc_snum++;
			mc->mc_top++;
			ptop++;
		}
		// Right page might now have changed parent.
		// Check if left page also changed parent.
		if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
		    mc->mc_ki[ptop] >= NUMKEYS(mc->mc_pg[ptop])) {
			for (i=0; i<ptop; i++) {
				mc->mc_pg[i] = mn.mc_pg[i];
				mc->mc_ki[i] = mn.mc_ki[i];
			}
			mc->mc_pg[ptop] = mn.mc_pg[ptop];
			mc->mc_ki[ptop] = mn.mc_ki[ptop] - 1;
		}
	} else {
		mn.mc_top--;
		rc = mdb_node_add(&mn, mn.mc_ki[ptop], &sepkey, NULL, rp->mp_pgno, 0);
		mn.mc_top++;
	}
	mc->mc_flags ^= C_SPLITTING;
	if (rc != MDB_SUCCESS) {
		return rc;
	}
	if (nflags & MDB_APPEND) {
		mc->mc_pg[mc->mc_top] = rp;
		mc->mc_ki[mc->mc_top] = 0;
		rc = mdb_node_add(mc, 0, newkey, newdata, newpgno, nflags);
		if (rc)
			return rc;
		for (i=0; i<mc->mc_top; i++)
			mc->mc_ki[i] = mn.mc_ki[i];
	} else if (!IS_LEAF2(mp)) {
		// Move nodes
		mc->mc_pg[mc->mc_top] = rp;
		i = split_indx;
		j = 0;
		do {
			if (i == newindx) {
				rkey.mv_data = newkey->mv_data;
				rkey.mv_size = newkey->mv_size;
				if (IS_LEAF(mp)) {
					rdata = newdata;
				} else
					pgno = newpgno;
				flags = nflags;
				// Update index for the new key.
				mc->mc_ki[mc->mc_top] = j;
			} else {
				node = (MDB_node *)((char *)mp + copy->mp_ptrs[i]);
				rkey.mv_data = NODEKEY(node);
				rkey.mv_size = node->mn_ksize;
				if (IS_LEAF(mp)) {
					xdata.mv_data = NODEDATA(node);
					xdata.mv_size = NODEDSZ(node);
					rdata = &xdata;
				} else
					pgno = NODEPGNO(node);
				flags = node->mn_flags;
			}

			if (!IS_LEAF(mp) && j == 0) {
				// First branch index doesn't need key data.
				rkey.mv_size = 0;
			}

			rc = mdb_node_add(mc, j, &rkey, rdata, pgno, flags);
			if (rc) {
				// return tmp page to freelist
				mdb_page_free(env, copy);
				return rc;
			}
			if (i == nkeys) {
				i = 0;
				j = 0;
				mc->mc_pg[mc->mc_top] = copy;
			} else {
				i++;
				j++;
			}
		} while (i != split_indx);

		nkeys = NUMKEYS(copy);
		for (i=0; i<nkeys; i++)
			mp->mp_ptrs[i] = copy->mp_ptrs[i];
		mp->mp_lower = copy->mp_lower;
		mp->mp_upper = copy->mp_upper;
		memcpy(NODEPTR(mp, nkeys-1), NODEPTR(copy, nkeys-1),
			env->me_psize - copy->mp_upper);

		// reset back to original page
		if (newindx < split_indx) {
			mc->mc_pg[mc->mc_top] = mp;
			if (nflags & MDB_RESERVE) {
				node = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
				if (!(node->mn_flags & F_BIGDATA))
					newdata->mv_data = NODEDATA(node);
			}
		} else {
			mc->mc_pg[mc->mc_top] = rp;
			mc->mc_ki[ptop]++;
			// Make sure mc_ki is still valid.
			if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
				mc->mc_ki[ptop] >= NUMKEYS(mc->mc_pg[ptop])) {
				for (i=0; i<ptop; i++) {
					mc->mc_pg[i] = mn.mc_pg[i];
					mc->mc_ki[i] = mn.mc_ki[i];
				}
				mc->mc_pg[ptop] = mn.mc_pg[ptop];
				mc->mc_ki[ptop] = mn.mc_ki[ptop] - 1;
			}
		}
		// return tmp page to freelist
		mdb_page_free(env, copy);
	}

	{
		// Adjust other cursors pointing to mp
		MDB_cursor *m2, *m3;
		MDB_dbi dbi = mc->mc_dbi;
		int fixup = NUMKEYS(mp);

		for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2=m2->mc_next) {
			if (mc->mc_flags & C_SUB)
				m3 = &m2->mc_xcursor->mx_cursor;
			else
				m3 = m2;
			if (m3 == mc)
				continue;
			if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
				continue;
			if (m3->mc_flags & C_SPLITTING)
				continue;
			if (new_root) {
				int k;
				// root split
				for (k=m3->mc_top; k>=0; k--) {
					m3->mc_ki[k+1] = m3->mc_ki[k];
					m3->mc_pg[k+1] = m3->mc_pg[k];
				}
				if (m3->mc_ki[0] >= split_indx) {
					m3->mc_ki[0] = 1;
				} else {
					m3->mc_ki[0] = 0;
				}
				m3->mc_pg[0] = mc->mc_pg[0];
				m3->mc_snum++;
				m3->mc_top++;
			}
			if (m3->mc_top >= mc->mc_top && m3->mc_pg[mc->mc_top] == mp) {
				if (m3->mc_ki[mc->mc_top] >= newindx && !(nflags & MDB_SPLIT_REPLACE))
					m3->mc_ki[mc->mc_top]++;
				if (m3->mc_ki[mc->mc_top] >= fixup) {
					m3->mc_pg[mc->mc_top] = rp;
					m3->mc_ki[mc->mc_top] -= fixup;
					m3->mc_ki[ptop] = mn.mc_ki[ptop];
				}
			} else if (!did_split && m3->mc_top >= ptop && m3->mc_pg[ptop] == mc->mc_pg[ptop] &&
				m3->mc_ki[ptop] >= mc->mc_ki[ptop]) {
				m3->mc_ki[ptop]++;
			}
		}
	}
	DPRINTF(("mp left: %d, rp left: %d", SIZELEFT(mp), SIZELEFT(rp)));
	return rc;
	*/
	return nil
}

// Add all the DB's pages to the free list.
// @param[in] mc Cursor on the DB to free.
// @param[in] subs non-Zero to check for sub-DBs in this DB.
// @return 0 on success, non-zero on failure.
func (c *cursor) drop0(subs int) error {
	/*
	int rc;

	rc = mdb_page_search(mc, NULL, MDB_PS_FIRST);
	if (rc == MDB_SUCCESS) {
		MDB_txn *txn = mc->mc_txn;
		MDB_node *ni;
		MDB_cursor mx;
		unsigned int i;

		// LEAF2 pages have no nodes, cannot have sub-DBs
		if (IS_LEAF2(mc->mc_pg[mc->mc_top]))
			mdb_cursor_pop(mc);

		mdb_cursor_copy(mc, &mx);
		while (mc->mc_snum > 0) {
			MDB_page *mp = mc->mc_pg[mc->mc_top];
			unsigned n = NUMKEYS(mp);
			if (IS_LEAF(mp)) {
				for (i=0; i<n; i++) {
					ni = NODEPTR(mp, i);
					if (ni->mn_flags & F_BIGDATA) {
						MDB_page *omp;
						pgno_t pg;
						memcpy(&pg, NODEDATA(ni), sizeof(pg));
						rc = mdb_page_get(txn, pg, &omp, NULL);
						if (rc != 0)
							return rc;
						mdb_cassert(mc, IS_OVERFLOW(omp));
						rc = mdb_midl_append_range(&txn->mt_free_pgs,
							pg, omp->mp_pages);
						if (rc)
							return rc;
					} else if (subs && (ni->mn_flags & F_SUBDATA)) {
						mdb_xcursor_init1(mc, ni);
						rc = mdb_drop0(&mc->mc_xcursor->mx_cursor, 0);
						if (rc)
							return rc;
					}
				}
			} else {
				if ((rc = mdb_midl_need(&txn->mt_free_pgs, n)) != 0)
					return rc;
				for (i=0; i<n; i++) {
					pgno_t pg;
					ni = NODEPTR(mp, i);
					pg = NODEPGNO(ni);
					// free it
					mdb_midl_xappend(txn->mt_free_pgs, pg);
				}
			}
			if (!mc->mc_top)
				break;
			mc->mc_ki[mc->mc_top] = i;
			rc = mdb_cursor_sibling(mc, 1);
			if (rc) {
				// no more siblings, go back to beginning
				// of previous level.
				mdb_cursor_pop(mc);
				mc->mc_ki[0] = 0;
				for (i=1; i<mc->mc_snum; i++) {
					mc->mc_ki[i] = 0;
					mc->mc_pg[i] = mx.mc_pg[i];
				}
			}
		}
		// free it
		rc = mdb_midl_append(&txn->mt_free_pgs, mc->mc_db->md_root);
	} else if (rc == MDB_NOTFOUND) {
		rc = MDB_SUCCESS;
	}
	return rc;
	*/
	return nil
}

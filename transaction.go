package bolt

// TODO: #define DB_DIRTY	0x01		/**< DB was modified or is DUPSORT data */
// TODO: #define DB_STALE	0x02		/**< Named-DB record is older than txnID */
// TODO: #define DB_NEW		0x04		/**< Named-DB handle opened in this txn */
// TODO: #define DB_VALID	0x08		/**< DB handle is valid, see also #MDB_VALID */

// TODO: #define MDB_TXN_RDONLY		0x01		/**< read-only transaction */
// TODO: #define MDB_TXN_ERROR		0x02		/**< an error has occurred */
// TODO: #define MDB_TXN_DIRTY		0x04		/**< must write, even if dirty list is empty */
// TODO: #define MDB_TXN_SPILLS		0x08		/**< txn or a parent has spilled pages */

type Transaction interface {
}

type transaction struct {
	id             int
	flags          int
	db             *DB
	parent         *transaction
	child          *transaction
	nextPageNumber int
	freePages      []int
	spillPages     []int
	dirtyList      []int
	reader         *reader
	// TODO: bucketxs []*bucketx
	buckets     []*Bucket
	bucketFlags []int
	cursors     []*cursor
	// Implicit from slices? TODO: MDB_dbi mt_numdbs;
	mt_dirty_room int
}

// ntxn represents a nested transaction.
type ntxn struct {
	transaction *transaction /**< the transaction */
	pageState   pageState    /**< parent transaction's saved freestate */
}

func (t *transaction) allocPage(num int) *page {
	/*
		MDB_env *env = txn->mt_env;
		MDB_page *ret = env->me_dpages;
		size_t psize = env->me_psize, sz = psize, off;
		// For ! #MDB_NOMEMINIT, psize counts how much to init.
		// For a single page alloc, we init everything after the page header.
		// For multi-page, we init the final page; if the caller needed that
		// many pages they will be filling in at least up to the last page.
		if (num == 1) {
			if (ret) {
				VGMEMP_ALLOC(env, ret, sz);
				VGMEMP_DEFINED(ret, sizeof(ret->mp_next));
				env->me_dpages = ret->mp_next;
				return ret;
			}
			psize -= off = PAGEHDRSZ;
		} else {
			sz *= num;
			off = sz - psize;
		}
		if ((ret = malloc(sz)) != NULL) {
			VGMEMP_ALLOC(env, ret, sz);
			if (!(env->me_flags & MDB_NOMEMINIT)) {
				memset((char *)ret + off, 0, psize);
				ret->mp_pad = 0;
			}
		} else {
			txn->mt_flags |= MDB_TXN_ERROR;
		}
		return ret;
	*/
	return nil
}

// Find oldest txnid still referenced. Expects txn->mt_txnid > 0.
func (t *transaction) oldest() int {
	/*
		int i;
		txnid_t mr, oldest = txn->mt_txnid - 1;
		if (txn->mt_env->me_txns) {
			MDB_reader *r = txn->mt_env->me_txns->mti_readers;
			for (i = txn->mt_env->me_txns->mti_numreaders; --i >= 0; ) {
				if (r[i].mr_pid) {
					mr = r[i].mr_txnid;
					if (oldest > mr)
						oldest = mr;
				}
			}
		}
		return oldest;
	*/
	return 0
}

// Add a page to the txn's dirty list
func (t *transaction) dirty(p *page) {
	/*
		MDB_ID2 mid;
		int rc, (*insert)(MDB_ID2L, MDB_ID2 *);

		if (txn->mt_env->me_flags & MDB_WRITEMAP) {
			insert = mdb_mid2l_append;
		} else {
			insert = mdb_mid2l_insert;
		}
		mid.mid = mp->mp_pgno;
		mid.mptr = mp;
		rc = insert(txn->mt_u.dirty_list, &mid);
		mdb_tassert(txn, rc == 0);
		txn->mt_dirty_room--;
	*/
}

// Pull a page off the txn's spill list, if present.
// If a page being referenced was spilled to disk in this txn, bring
// it back and make it dirty/writable again.
// @param[in] txn the transaction handle.
// @param[in] mp the page being referenced. It must not be dirty.
// @param[out] ret the writable page, if any. ret is unchanged if
// mp wasn't spilled.
func (t *transaction) unspill(p *page) *page {
	/*
		MDB_env *env = txn->mt_env;
		const MDB_txn *tx2;
		unsigned x;
		pgno_t pgno = mp->mp_pgno, pn = pgno << 1;

		for (tx2 = txn; tx2; tx2=tx2->mt_parent) {
			if (!tx2->mt_spill_pgs)
				continue;
			x = mdb_midl_search(tx2->mt_spill_pgs, pn);
			if (x <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[x] == pn) {
				MDB_page *np;
				int num;
				if (txn->mt_dirty_room == 0)
					return MDB_TXN_FULL;
				if (IS_OVERFLOW(mp))
					num = mp->mp_pages;
				else
					num = 1;
				if (env->me_flags & MDB_WRITEMAP) {
					np = mp;
				} else {
					np = mdb_page_malloc(txn, num);
					if (!np)
						return ENOMEM;
					if (num > 1)
						memcpy(np, mp, num * env->me_psize);
					else
						mdb_page_copy(np, mp, env->me_psize);
				}
				if (tx2 == txn) {
					// If in current txn, this page is no longer spilled.
					// If it happens to be the last page, truncate the spill list.
					// Otherwise mark it as deleted by setting the LSB.
					if (x == txn->mt_spill_pgs[0])
						txn->mt_spill_pgs[0]--;
					else
						txn->mt_spill_pgs[x] |= 1;
				}	// otherwise, if belonging to a parent txn, the
					// page remains spilled until child commits

				mdb_page_dirty(txn, np);
				np->mp_flags |= P_DIRTY;
				*ret = np;
				break;
			}
		}
		return MDB_SUCCESS;
	*/
	return nil
}

// Back up parent txn's cursors, then grab the originals for tracking
func (t *transaction) shadow(dst *transaction) error {
	/*
		MDB_cursor *mc, *bk;
		MDB_xcursor *mx;
		size_t size;
		int i;

		for (i = src->mt_numdbs; --i >= 0; ) {
			if ((mc = src->mt_cursors[i]) != NULL) {
				size = sizeof(MDB_cursor);
				if (mc->mc_xcursor)
					size += sizeof(MDB_xcursor);
				for (; mc; mc = bk->mc_next) {
					bk = malloc(size);
					if (!bk)
						return ENOMEM;
					*bk = *mc;
					mc->mc_backup = bk;
					mc->mc_db = &dst->mt_dbs[i];
					// Kill pointers into src - and dst to reduce abuse: The
					// user may not use mc until dst ends. Otherwise we'd...
					mc->mc_txn    = NULL;	// ...set this to dst
					mc->mc_dbflag = NULL;	// ...and &dst->mt_dbflags[i]
					if ((mx = mc->mc_xcursor) != NULL) {
						*(MDB_xcursor *)(bk+1) = *mx;
						mx->mx_cursor.mc_txn = NULL; // ...and dst.
					}
					mc->mc_next = dst->mt_cursors[i];
					dst->mt_cursors[i] = mc;
				}
			}
		}
		return MDB_SUCCESS;
	*/
	return nil
}

// Close this write txn's cursors, give parent txn's cursors back to parent.
// @param[in] txn the transaction handle.
// @param[in] merge true to keep changes to parent cursors, false to revert.
// @return 0 on success, non-zero on failure.
func (t *transaction) closeCursors(merge bool) {
	/*
		MDB_cursor **cursors = txn->mt_cursors, *mc, *next, *bk;
		MDB_xcursor *mx;
		int i;

		for (i = txn->mt_numdbs; --i >= 0; ) {
			for (mc = cursors[i]; mc; mc = next) {
				next = mc->mc_next;
				if ((bk = mc->mc_backup) != NULL) {
					if (merge) {
						// Commit changes to parent txn
						mc->mc_next = bk->mc_next;
						mc->mc_backup = bk->mc_backup;
						mc->mc_txn = bk->mc_txn;
						mc->mc_db = bk->mc_db;
						mc->mc_dbflag = bk->mc_dbflag;
						if ((mx = mc->mc_xcursor) != NULL)
							mx->mx_cursor.mc_txn = bk->mc_txn;
					} else {
						// Abort nested txn
						*mc = *bk;
						if ((mx = mc->mc_xcursor) != NULL)
							*mx = *(MDB_xcursor *)(bk+1);
					}
					mc = bk;
				}
				// Only malloced cursors are permanently tracked.
				free(mc);
			}
			cursors[i] = NULL;
		}
	*/
}

// Common code for #mdb_txn_begin() and #mdb_txn_renew().
// @param[in] txn the transaction handle to initialize
// @return 0 on success, non-zero on failure.
func (t *transaction) renew() error {
	/*
			MDB_env *env = txn->mt_env;
			MDB_txninfo *ti = env->me_txns;
			MDB_meta *meta;
			unsigned int i, nr;
			uint16_t x;
			int rc, new_notls = 0;

			// Setup db info 
			txn->mt_numdbs = env->me_numdbs;
			txn->mt_dbxs = env->me_dbxs;	// mostly static anyway 

			if (txn->mt_flags & MDB_TXN_RDONLY) {
				if (!ti) {
					meta = env->me_metas[ mdb_env_pick_meta(env) ];
					txn->mt_txnid = meta->mm_txnid;
					txn->mt_u.reader = NULL;
				} else {
					MDB_reader *r = (env->me_flags & MDB_NOTLS) ? txn->mt_u.reader :
						pthread_getspecific(env->me_txkey);
					if (r) {
						if (r->mr_pid != env->me_pid || r->mr_txnid != (txnid_t)-1)
							return MDB_BAD_RSLOT;
					} else {
						MDB_PID_T pid = env->me_pid;
						pthread_t tid = pthread_self();

						if (!(env->me_flags & MDB_LIVE_READER)) {
							rc = mdb_reader_pid(env, Pidset, pid);
							if (rc)
								return rc;
							env->me_flags |= MDB_LIVE_READER;
						}

						LOCK_MUTEX_R(env);
						nr = ti->mti_numreaders;
						for (i=0; i<nr; i++)
							if (ti->mti_readers[i].mr_pid == 0)
								break;
						if (i == env->me_maxreaders) {
							UNLOCK_MUTEX_R(env);
							return MDB_READERS_FULL;
						}
						ti->mti_readers[i].mr_pid = pid;
						ti->mti_readers[i].mr_tid = tid;
						if (i == nr)
							ti->mti_numreaders = ++nr;
						// Save numreaders for un-mutexed mdb_env_close() 
						env->me_numreaders = nr;
						UNLOCK_MUTEX_R(env);

						r = &ti->mti_readers[i];
						new_notls = (env->me_flags & MDB_NOTLS);
						if (!new_notls && (rc=pthread_setspecific(env->me_txkey, r))) {
							r->mr_pid = 0;
							return rc;
						}
					}
					txn->mt_txnid = r->mr_txnid = ti->mti_txnid;
					txn->mt_u.reader = r;
					meta = env->me_metas[txn->mt_txnid & 1];
				}
			} else {
				if (ti) {
					LOCK_MUTEX_W(env);

					txn->mt_txnid = ti->mti_txnid;
					meta = env->me_metas[txn->mt_txnid & 1];
				} else {
					meta = env->me_metas[ mdb_env_pick_meta(env) ];
					txn->mt_txnid = meta->mm_txnid;
				}
				txn->mt_txnid++;
		#if MDB_DEBUG
				if (txn->mt_txnid == mdb_debug_start)
					mdb_debug = 1;
		#endif
				txn->mt_dirty_room = MDB_IDL_UM_MAX;
				txn->mt_u.dirty_list = env->me_dirty_list;
				txn->mt_u.dirty_list[0].mid = 0;
				txn->mt_free_pgs = env->me_free_pgs;
				txn->mt_free_pgs[0] = 0;
				txn->mt_spill_pgs = NULL;
				env->me_txn = txn;
			}

			// Copy the DB info and flags 
			memcpy(txn->mt_dbs, meta->mm_dbs, 2 * sizeof(MDB_db));

			// Moved to here to avoid a data race in read TXNs 
			txn->mt_next_pgno = meta->mm_last_pg+1;

			for (i=2; i<txn->mt_numdbs; i++) {
				x = env->me_dbflags[i];
				txn->mt_dbs[i].md_flags = x & PERSISTENT_FLAGS;
				txn->mt_dbflags[i] = (x & MDB_VALID) ? DB_VALID|DB_STALE : 0;
			}
			txn->mt_dbflags[0] = txn->mt_dbflags[1] = DB_VALID;

			if (env->me_maxpg < txn->mt_next_pgno) {
				mdb_txn_reset0(txn, "renew0-mapfail");
				if (new_notls) {
					txn->mt_u.reader->mr_pid = 0;
					txn->mt_u.reader = NULL;
				}
				return MDB_MAP_RESIZED;
			}

			return MDB_SUCCESS;
	*/
	return nil
}

func (t *transaction) Renew() error {
	/*
		int rc;

		if (!txn || txn->mt_dbxs)	// A reset txn has mt_dbxs==NULL
			return EINVAL;

		if (txn->mt_env->me_flags & MDB_FATAL_ERROR) {
			DPUTS("environment had fatal error, must shutdown!");
			return MDB_PANIC;
		}

		rc = mdb_txn_renew0(txn);
		if (rc == MDB_SUCCESS) {
			DPRINTF(("renew txn %"Z"u%c %p on mdbenv %p, root page %"Z"u",
				txn->mt_txnid, (txn->mt_flags & MDB_TXN_RDONLY) ? 'r' : 'w',
				(void *)txn, (void *)txn->mt_env, txn->mt_dbs[MAIN_DBI].md_root));
		}
		return rc;
	*/
	return nil
}

func (t *transaction) DB() *DB {
	return t.db
}

// Export or close DBI handles opened in this txn.
func (t *transaction) updateBuckets(keep bool) {
	/*
		int i;
		MDB_dbi n = txn->mt_numdbs;
		MDB_env *env = txn->mt_env;
		unsigned char *tdbflags = txn->mt_dbflags;

		for (i = n; --i >= 2;) {
			if (tdbflags[i] & DB_NEW) {
				if (keep) {
					env->me_dbflags[i] = txn->mt_dbs[i].md_flags | MDB_VALID;
				} else {
					char *ptr = env->me_dbxs[i].md_name.mv_data;
					env->me_dbxs[i].md_name.mv_data = NULL;
					env->me_dbxs[i].md_name.mv_size = 0;
					env->me_dbflags[i] = 0;
					free(ptr);
				}
			}
		}
		if (keep && env->me_numdbs < n)
			env->me_numdbs = n;
	*/
}

// Common code for #mdb_txn_reset() and #mdb_txn_abort().
// May be called twice for readonly txns: First reset it, then abort.
// @param[in] txn the transaction handle to reset
// @param[in] act why the transaction is being reset
func (t *transaction) reset(act string) {
	/*
		MDB_env	*env = txn->mt_env;

		// Close any DBI handles opened in this txn
		mdb_dbis_update(txn, 0);

		DPRINTF(("%s txn %"Z"u%c %p on mdbenv %p, root page %"Z"u",
			act, txn->mt_txnid, (txn->mt_flags & MDB_TXN_RDONLY) ? 'r' : 'w',
			(void *) txn, (void *)env, txn->mt_dbs[MAIN_DBI].md_root));

		if (F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)) {
			if (txn->mt_u.reader) {
				txn->mt_u.reader->mr_txnid = (txnid_t)-1;
				if (!(env->me_flags & MDB_NOTLS))
					txn->mt_u.reader = NULL; // txn does not own reader
			}
			txn->mt_numdbs = 0;		// close nothing if called again
			txn->mt_dbxs = NULL;	// mark txn as reset
		} else {
			mdb_cursors_close(txn, 0);

			if (!(env->me_flags & MDB_WRITEMAP)) {
				mdb_dlist_free(txn);
			}
			mdb_midl_free(env->me_pghead);

			if (txn->mt_parent) {
				txn->mt_parent->mt_child = NULL;
				env->me_pgstate = ((MDB_ntxn *)txn)->mnt_pgstate;
				mdb_midl_free(txn->mt_free_pgs);
				mdb_midl_free(txn->mt_spill_pgs);
				free(txn->mt_u.dirty_list);
				return;
			}

			if (mdb_midl_shrink(&txn->mt_free_pgs))
				env->me_free_pgs = txn->mt_free_pgs;
			env->me_pghead = NULL;
			env->me_pglast = 0;

			env->me_txn = NULL;
			// The writer mutex was locked in mdb_txn_begin.
			if (env->me_txns)
				UNLOCK_MUTEX_W(env);
		}
	*/
}

func (t *transaction) Reset() {
	/*
		if (txn == NULL)
			return;

		// This call is only valid for read-only txns
		if (!(txn->mt_flags & MDB_TXN_RDONLY))
			return;

		mdb_txn_reset0(txn, "reset");
	*/
}

func (t *transaction) Abort() {
	/*
		if (txn == NULL)
			return;

		if (txn->mt_child)
			mdb_txn_abort(txn->mt_child);

		mdb_txn_reset0(txn, "abort");
		// Free reader slot tied to this txn (if MDB_NOTLS && writable FS)
		if ((txn->mt_flags & MDB_TXN_RDONLY) && txn->mt_u.reader)
			txn->mt_u.reader->mr_pid = 0;

		free(txn);
	*/
}

// Save the freelist as of this transaction to the freeDB.
// This changes the freelist. Keep trying until it stabilizes.
func (t *transaction) saveFreelist() error {
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

// Flush (some) dirty pages to the map, after clearing their dirty flag.
// @param[in] txn the transaction that's being committed
// @param[in] keep number of initial pages in dirty_list to keep dirty.
// @return 0 on success, non-zero on failure.
func (t *transaction) flush(keep bool) error {
	/*
			MDB_env		*env = txn->mt_env;
			MDB_ID2L	dl = txn->mt_u.dirty_list;
			unsigned	psize = env->me_psize, j;
			int			i, pagecount = dl[0].mid, rc;
			size_t		size = 0, pos = 0;
			pgno_t		pgno = 0;
			MDB_page	*dp = NULL;
		#ifdef _WIN32
			OVERLAPPED	ov;
		#else
			struct iovec iov[MDB_COMMIT_PAGES];
			ssize_t		wpos = 0, wsize = 0, wres;
			size_t		next_pos = 1; // impossible pos, so pos != next_pos
			int			n = 0;
		#endif

			j = i = keep;

			if (env->me_flags & MDB_WRITEMAP) {
				// Clear dirty flags 
				while (++i <= pagecount) {
					dp = dl[i].mptr;
					// Don't flush this page yet 
					if (dp->mp_flags & P_KEEP) {
						dp->mp_flags ^= P_KEEP;
						dl[++j] = dl[i];
						continue;
					}
					dp->mp_flags &= ~P_DIRTY;
				}
				goto done;
			}

			// Write the pages
			for (;;) {
				if (++i <= pagecount) {
					dp = dl[i].mptr;
					// Don't flush this page yet
					if (dp->mp_flags & P_KEEP) {
						dp->mp_flags ^= P_KEEP;
						dl[i].mid = 0;
						continue;
					}
					pgno = dl[i].mid;
					// clear dirty flag
					dp->mp_flags &= ~P_DIRTY;
					pos = pgno * psize;
					size = psize;
					if (IS_OVERFLOW(dp)) size *= dp->mp_pages;
				}
		#ifdef _WIN32
				else break;

				// Windows actually supports scatter/gather I/O, but only on
				// unbuffered file handles. Since we're relying on the OS page
				// cache for all our data, that's self-defeating. So we just
				// write pages one at a time. We use the ov structure to set
				// the write offset, to at least save the overhead of a Seek
				// system call.
				DPRINTF(("committing page %"Z"u", pgno));
				memset(&ov, 0, sizeof(ov));
				ov.Offset = pos & 0xffffffff;
				ov.OffsetHigh = pos >> 16 >> 16;
				if (!WriteFile(env->me_fd, dp, size, NULL, &ov)) {
					rc = ErrCode();
					DPRINTF(("WriteFile: %d", rc));
					return rc;
				}
		#else
				// Write up to MDB_COMMIT_PAGES dirty pages at a time.
				if (pos!=next_pos || n==MDB_COMMIT_PAGES || wsize+size>MAX_WRITE) {
					if (n) {
						// Write previous page(s)
		#ifdef MDB_USE_PWRITEV
						wres = pwritev(env->me_fd, iov, n, wpos);
		#else
						if (n == 1) {
							wres = pwrite(env->me_fd, iov[0].iov_base, wsize, wpos);
						} else {
							if (lseek(env->me_fd, wpos, SEEK_SET) == -1) {
								rc = ErrCode();
								DPRINTF(("lseek: %s", strerror(rc)));
								return rc;
							}
							wres = writev(env->me_fd, iov, n);
						}
		#endif
						if (wres != wsize) {
							if (wres < 0) {
								rc = ErrCode();
								DPRINTF(("Write error: %s", strerror(rc)));
							} else {
								rc = EIO; // TODO: Use which error code?
								DPUTS("short write, filesystem full?");
							}
							return rc;
						}
						n = 0;
					}
					if (i > pagecount)
						break;
					wpos = pos;
					wsize = 0;
				}
				DPRINTF(("committing page %"Z"u", pgno));
				next_pos = pos + size;
				iov[n].iov_len = size;
				iov[n].iov_base = (char *)dp;
				wsize += size;
				n++;
		#endif	// _WIN32
			}

			for (i = keep; ++i <= pagecount; ) {
				dp = dl[i].mptr;
				// This is a page we skipped above
				if (!dl[i].mid) {
					dl[++j] = dl[i];
					dl[j].mid = dp->mp_pgno;
					continue;
				}
				mdb_dpage_free(env, dp);
			}

		done:
			i--;
			txn->mt_dirty_room += i - j;
			dl[0].mid = j;
			return MDB_SUCCESS;
		}

		int
		mdb_txn_commit(MDB_txn *txn)
		{
			int		rc;
			unsigned int i;
			MDB_env	*env;

			if (txn == NULL || txn->mt_env == NULL)
				return EINVAL;

			if (txn->mt_child) {
				rc = mdb_txn_commit(txn->mt_child);
				txn->mt_child = NULL;
				if (rc)
					goto fail;
			}

			env = txn->mt_env;

			if (F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)) {
				mdb_dbis_update(txn, 1);
				txn->mt_numdbs = 2; // so txn_abort() doesn't close any new handles
				mdb_txn_abort(txn);
				return MDB_SUCCESS;
			}

			if (F_ISSET(txn->mt_flags, MDB_TXN_ERROR)) {
				DPUTS("error flag is set, can't commit");
				if (txn->mt_parent)
					txn->mt_parent->mt_flags |= MDB_TXN_ERROR;
				rc = MDB_BAD_TXN;
				goto fail;
			}

			if (txn->mt_parent) {
				MDB_txn *parent = txn->mt_parent;
				MDB_ID2L dst, src;
				MDB_IDL pspill;
				unsigned x, y, len, ps_len;

				// Append our free list to parent's
				rc = mdb_midl_append_list(&parent->mt_free_pgs, txn->mt_free_pgs);
				if (rc)
					goto fail;
				mdb_midl_free(txn->mt_free_pgs);
				// Failures after this must either undo the changes
				// to the parent or set MDB_TXN_ERROR in the parent.

				parent->mt_next_pgno = txn->mt_next_pgno;
				parent->mt_flags = txn->mt_flags;

				// Merge our cursors into parent's and close them
				mdb_cursors_close(txn, 1);

				// Update parent's DB table.
				memcpy(parent->mt_dbs, txn->mt_dbs, txn->mt_numdbs * sizeof(MDB_db));
				parent->mt_numdbs = txn->mt_numdbs;
				parent->mt_dbflags[0] = txn->mt_dbflags[0];
				parent->mt_dbflags[1] = txn->mt_dbflags[1];
				for (i=2; i<txn->mt_numdbs; i++) {
					// preserve parent's DB_NEW status
					x = parent->mt_dbflags[i] & DB_NEW;
					parent->mt_dbflags[i] = txn->mt_dbflags[i] | x;
				}

				dst = parent->mt_u.dirty_list;
				src = txn->mt_u.dirty_list;
				// Remove anything in our dirty list from parent's spill list
				if ((pspill = parent->mt_spill_pgs) && (ps_len = pspill[0])) {
					x = y = ps_len;
					pspill[0] = (pgno_t)-1;
					// Mark our dirty pages as deleted in parent spill list 
					for (i=0, len=src[0].mid; ++i <= len; ) {
						MDB_ID pn = src[i].mid << 1;
						while (pn > pspill[x])
							x--;
						if (pn == pspill[x]) {
							pspill[x] = 1;
							y = --x;
						}
					}
					// Squash deleted pagenums if we deleted any
					for (x=y; ++x <= ps_len; )
						if (!(pspill[x] & 1))
							pspill[++y] = pspill[x];
					pspill[0] = y;
				}

				// Find len = length of merging our dirty list with parent's
				x = dst[0].mid;
				dst[0].mid = 0;		// simplify loops
				if (parent->mt_parent) {
					len = x + src[0].mid;
					y = mdb_mid2l_search(src, dst[x].mid + 1) - 1;
					for (i = x; y && i; y--) {
						pgno_t yp = src[y].mid;
						while (yp < dst[i].mid)
							i--;
						if (yp == dst[i].mid) {
							i--;
							len--;
						}
					}
				} else { // Simplify the above for single-ancestor case
					len = MDB_IDL_UM_MAX - txn->mt_dirty_room;
				}
				// Merge our dirty list with parent's 
				y = src[0].mid;
				for (i = len; y; dst[i--] = src[y--]) {
					pgno_t yp = src[y].mid;
					while (yp < dst[x].mid)
						dst[i--] = dst[x--];
					if (yp == dst[x].mid)
						free(dst[x--].mptr);
				}
				mdb_tassert(txn, i == x);
				dst[0].mid = len;
				free(txn->mt_u.dirty_list);
				parent->mt_dirty_room = txn->mt_dirty_room;
				if (txn->mt_spill_pgs) {
					if (parent->mt_spill_pgs) {
						// TODO: Prevent failure here, so parent does not fail 
						rc = mdb_midl_append_list(&parent->mt_spill_pgs, txn->mt_spill_pgs);
						if (rc)
							parent->mt_flags |= MDB_TXN_ERROR;
						mdb_midl_free(txn->mt_spill_pgs);
						mdb_midl_sort(parent->mt_spill_pgs);
					} else {
						parent->mt_spill_pgs = txn->mt_spill_pgs;
					}
				}

				parent->mt_child = NULL;
				mdb_midl_free(((MDB_ntxn *)txn)->mnt_pgstate.mf_pghead);
				free(txn);
				return rc;
			}

			if (txn != env->me_txn) {
				DPUTS("attempt to commit unknown transaction");
				rc = EINVAL;
				goto fail;
			}

			mdb_cursors_close(txn, 0);

			if (!txn->mt_u.dirty_list[0].mid &&
				!(txn->mt_flags & (MDB_TXN_DIRTY|MDB_TXN_SPILLS)))
				goto done;

			DPRINTF(("committing txn %"Z"u %p on mdbenv %p, root page %"Z"u",
			    txn->mt_txnid, (void*)txn, (void*)env, txn->mt_dbs[MAIN_DBI].md_root));

			// Update DB root pointers
			if (txn->mt_numdbs > 2) {
				MDB_cursor mc;
				MDB_dbi i;
				MDB_val data;
				data.mv_size = sizeof(MDB_db);

				mdb_cursor_init(&mc, txn, MAIN_DBI, NULL);
				for (i = 2; i < txn->mt_numdbs; i++) {
					if (txn->mt_dbflags[i] & DB_DIRTY) {
						data.mv_data = &txn->mt_dbs[i];
						rc = mdb_cursor_put(&mc, &txn->mt_dbxs[i].md_name, &data, 0);
						if (rc)
							goto fail;
					}
				}
			}

			rc = mdb_freelist_save(txn);
			if (rc)
				goto fail;

			mdb_midl_free(env->me_pghead);
			env->me_pghead = NULL;
			if (mdb_midl_shrink(&txn->mt_free_pgs))
				env->me_free_pgs = txn->mt_free_pgs;

		#if (MDB_DEBUG) > 2
			mdb_audit(txn);
		#endif

			if ((rc = mdb_page_flush(txn, 0)) ||
				(rc = mdb_env_sync(env, 0)) ||
				(rc = mdb_env_write_meta(txn)))
				goto fail;

		done:
			env->me_pglast = 0;
			env->me_txn = NULL;
			mdb_dbis_update(txn, 1);

			if (env->me_txns)
				UNLOCK_MUTEX_W(env);
			free(txn);

			return MDB_SUCCESS;

		fail:
			mdb_txn_abort(txn);
			return rc;
	*/
	return nil
}

// Update the environment info to commit a transaction.
// @param[in] txn the transaction that's being committed
// @return 0 on success, non-zero on failure.
func (t *transaction) writeMeta() error {
	/*
			MDB_env *env;
			MDB_meta	meta, metab, *mp;
			off_t off;
			int rc, len, toggle;
			char *ptr;
			HANDLE mfd;
		#ifdef _WIN32
			OVERLAPPED ov;
		#else
			int r2;
		#endif

			toggle = txn->mt_txnid & 1;
			DPRINTF(("writing meta page %d for root page %"Z"u",
				toggle, txn->mt_dbs[MAIN_DBI].md_root));

			env = txn->mt_env;
			mp = env->me_metas[toggle];

			if (env->me_flags & MDB_WRITEMAP) {
				// Persist any increases of mapsize config
				if (env->me_mapsize > mp->mm_mapsize)
					mp->mm_mapsize = env->me_mapsize;
				mp->mm_dbs[0] = txn->mt_dbs[0];
				mp->mm_dbs[1] = txn->mt_dbs[1];
				mp->mm_last_pg = txn->mt_next_pgno - 1;
				mp->mm_txnid = txn->mt_txnid;
				if (!(env->me_flags & (MDB_NOMETASYNC|MDB_NOSYNC))) {
					unsigned meta_size = env->me_psize;
					rc = (env->me_flags & MDB_MAPASYNC) ? MS_ASYNC : MS_SYNC;
					ptr = env->me_map;
					if (toggle) {
		#ifndef _WIN32	// POSIX msync() requires ptr = start of OS page
						if (meta_size < env->me_os_psize)
							meta_size += meta_size;
						else
		#endif
							ptr += meta_size;
					}
					if (MDB_MSYNC(ptr, meta_size, rc)) {
						rc = ErrCode();
						goto fail;
					}
				}
				goto done;
			}
			metab.mm_txnid = env->me_metas[toggle]->mm_txnid;
			metab.mm_last_pg = env->me_metas[toggle]->mm_last_pg;

			ptr = (char *)&meta;
			if (env->me_mapsize > mp->mm_mapsize) {
				// Persist any increases of mapsize config
				meta.mm_mapsize = env->me_mapsize;
				off = offsetof(MDB_meta, mm_mapsize);
			} else {
				off = offsetof(MDB_meta, mm_dbs[0].md_depth);
			}
			len = sizeof(MDB_meta) - off;

			ptr += off;
			meta.mm_dbs[0] = txn->mt_dbs[0];
			meta.mm_dbs[1] = txn->mt_dbs[1];
			meta.mm_last_pg = txn->mt_next_pgno - 1;
			meta.mm_txnid = txn->mt_txnid;

			if (toggle)
				off += env->me_psize;
			off += PAGEHDRSZ;

			// Write to the SYNC fd
			mfd = env->me_flags & (MDB_NOSYNC|MDB_NOMETASYNC) ?
				env->me_fd : env->me_mfd;
		#ifdef _WIN32
			{
				memset(&ov, 0, sizeof(ov));
				ov.Offset = off;
				if (!WriteFile(mfd, ptr, len, (DWORD *)&rc, &ov))
					rc = -1;
			}
		#else
			rc = pwrite(mfd, ptr, len, off);
		#endif
			if (rc != len) {
				rc = rc < 0 ? ErrCode() : EIO;
				DPUTS("write failed, disk error?");
				// On a failure, the pagecache still contains the new data.
				// Write some old data back, to prevent it from being used.
				// Use the non-SYNC fd; we know it will fail anyway.
				meta.mm_last_pg = metab.mm_last_pg;
				meta.mm_txnid = metab.mm_txnid;
		#ifdef _WIN32
				memset(&ov, 0, sizeof(ov));
				ov.Offset = off;
				WriteFile(env->me_fd, ptr, len, NULL, &ov);
		#else
				r2 = pwrite(env->me_fd, ptr, len, off);
				(void)r2;	// Silence warnings. We don't care about pwrite's return value
		#endif
		fail:
				env->me_flags |= MDB_FATAL_ERROR;
				return rc;
			}
		done:
			// Memory ordering issues are irrelevant; since the entire writer
			// is wrapped by wmutex, all of these changes will become visible
			// after the wmutex is unlocked. Since the DB is multi-version,
			// readers will get consistent data regardless of how fresh or
			// how stale their view of these values is.
			if (env->me_txns)
				env->me_txns->mti_txnid = txn->mt_txnid;

			return MDB_SUCCESS;
	*/
	return nil
}

// Find the address of the page corresponding to a given page number.
// @param[in] txn the transaction for this access.
// @param[in] pgno the page number for the page to retrieve.
// @param[out] ret address of a pointer where the page's address will be stored.
// @param[out] lvl dirty_list inheritance level of found page. 1=current txn, 0=mapped page.
// @return 0 on success, non-zero on failure.
func (t *transaction) getPage(id int) (*page, int, error) {
	/*
			MDB_env *env = txn->mt_env;
			MDB_page *p = NULL;
			int level;

			if (!((txn->mt_flags & MDB_TXN_RDONLY) | (env->me_flags & MDB_WRITEMAP))) {
				MDB_txn *tx2 = txn;
				level = 1;
				do {
					MDB_ID2L dl = tx2->mt_u.dirty_list;
					unsigned x;
					// Spilled pages were dirtied in this txn and flushed
					// because the dirty list got full. Bring this page
					// back in from the map (but don't unspill it here,
					// leave that unless page_touch happens again).
					if (tx2->mt_spill_pgs) {
						MDB_ID pn = pgno << 1;
						x = mdb_midl_search(tx2->mt_spill_pgs, pn);
						if (x <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[x] == pn) {
							p = (MDB_page *)(env->me_map + env->me_psize * pgno);
							goto done;
						}
					}
					if (dl[0].mid) {
						unsigned x = mdb_mid2l_search(dl, pgno);
						if (x <= dl[0].mid && dl[x].mid == pgno) {
							p = dl[x].mptr;
							goto done;
						}
					}
					level++;
				} while ((tx2 = tx2->mt_parent) != NULL);
			}

			if (pgno < txn->mt_next_pgno) {
				level = 0;
				p = (MDB_page *)(env->me_map + env->me_psize * pgno);
			} else {
				DPRINTF(("page %"Z"u not found", pgno));
				txn->mt_flags |= MDB_TXN_ERROR;
				return MDB_PAGE_NOTFOUND;
			}

		done:
			*ret = p;
			if (lvl)
				*lvl = level;
			return MDB_SUCCESS;
	*/

	return nil, 0, nil
}

// Return the data associated with a given node.
// @param[in] txn The transaction for this operation.
// @param[in] leaf The node being read.
// @param[out] data Updated to point to the node's data.
// @return 0 on success, non-zero on failure.
func (t *transaction) readNode(leaf *node, data []byte) error {
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

func (t *transaction) Get(bucket Bucket, key []byte) ([]byte, error) {
	/*
		MDB_cursor	mc;
		MDB_xcursor	mx;
		int exact = 0;
		DKBUF;

		if (key == NULL || data == NULL)
			return EINVAL;

		DPRINTF(("===> get db %u key [%s]", dbi, DKEY(key)));

		if (txn == NULL || !dbi || dbi >= txn->mt_numdbs || !(txn->mt_dbflags[dbi] & DB_VALID))
			return EINVAL;

		if (txn->mt_flags & MDB_TXN_ERROR)
			return MDB_BAD_TXN;

		mdb_cursor_init(&mc, txn, dbi, &mx);
		return mdb_cursor_set(&mc, key, data, MDB_SET, &exact);
	*/
	return nil, nil
}

func (t *transaction) Cursor(b Bucket) (Cursor, error) {
	/*
		MDB_cursor	*mc;
		size_t size = sizeof(MDB_cursor);

		if (txn == NULL || ret == NULL || dbi >= txn->mt_numdbs || !(txn->mt_dbflags[dbi] & DB_VALID))
			return EINVAL;

		if (txn->mt_flags & MDB_TXN_ERROR)
			return MDB_BAD_TXN;

		// Allow read access to the freelist
		if (!dbi && !F_ISSET(txn->mt_flags, MDB_TXN_RDONLY))
			return EINVAL;

		if (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT)
			size += sizeof(MDB_xcursor);

		if ((mc = malloc(size)) != NULL) {
			mdb_cursor_init(mc, txn, dbi, (MDB_xcursor *)(mc + 1));
			if (txn->mt_cursors) {
				mc->mc_next = txn->mt_cursors[dbi];
				txn->mt_cursors[dbi] = mc;
				mc->mc_flags |= C_UNTRACK;
			}
		} else {
			return ENOMEM;
		}

		*ret = mc;

		return MDB_SUCCESS;
	*/
	return nil, nil
}

func (t *transaction) Renew1(c Cursor) error {
	/*
		if (txn == NULL || mc == NULL || mc->mc_dbi >= txn->mt_numdbs)
			return EINVAL;

		if ((mc->mc_flags & C_UNTRACK) || txn->mt_cursors)
			return EINVAL;

		mdb_cursor_init(mc, txn, mc->mc_dbi, mc->mc_xcursor);
		return MDB_SUCCESS;
	*/
	return nil
}

func (t *transaction) Delete(b *Bucket, key []byte, data []byte) error {
	/*
		MDB_cursor mc;
		MDB_xcursor mx;
		MDB_cursor_op op;
		MDB_val rdata, *xdata;
		int		 rc, exact;
		DKBUF;

		if (key == NULL)
			return EINVAL;

		DPRINTF(("====> delete db %u key [%s]", dbi, DKEY(key)));

		if (txn == NULL || !dbi || dbi >= txn->mt_numdbs || !(txn->mt_dbflags[dbi] & DB_VALID))
			return EINVAL;

		if (txn->mt_flags & (MDB_TXN_RDONLY|MDB_TXN_ERROR))
			return (txn->mt_flags & MDB_TXN_RDONLY) ? EACCES : MDB_BAD_TXN;

		mdb_cursor_init(&mc, txn, dbi, &mx);

		exact = 0;
		if (!F_ISSET(txn->mt_dbs[dbi].md_flags, MDB_DUPSORT)) {
			// must ignore any data
			data = NULL;
		}
		if (data) {
			op = MDB_GET_BOTH;
			rdata = *data;
			xdata = &rdata;
		} else {
			op = MDB_SET;
			xdata = NULL;
		}
		rc = mdb_cursor_set(&mc, key, xdata, op, &exact);
		if (rc == 0) {
			// let mdb_page_split know about this cursor if needed:
			// delete will trigger a rebalance; if it needs to move
			// a node from one page to another, it will have to
			// update the parent's separator key(s). If the new sepkey
			// is larger than the current one, the parent page may
			// run out of space, triggering a split. We need this
			// cursor to be consistent until the end of the rebalance.
			mc.mc_flags |= C_UNTRACK;
			mc.mc_next = txn->mt_cursors[dbi];
			txn->mt_cursors[dbi] = &mc;
			rc = mdb_cursor_del(&mc, data ? 0 : MDB_NODUPDATA);
			txn->mt_cursors[dbi] = mc.mc_next;
		}
		return rc;
	*/
	return nil
}

func (t *transaction) Put(b Bucket, key []byte, data []byte, flags int) error {
	/*
		MDB_cursor mc;
		MDB_xcursor mx;

		if (key == NULL || data == NULL)
			return EINVAL;

		if (txn == NULL || !dbi || dbi >= txn->mt_numdbs || !(txn->mt_dbflags[dbi] & DB_VALID))
			return EINVAL;

		if ((flags & (MDB_NOOVERWRITE|MDB_NODUPDATA|MDB_RESERVE|MDB_APPEND|MDB_APPENDDUP)) != flags)
			return EINVAL;

		mdb_cursor_init(&mc, txn, dbi, &mx);
		return mdb_cursor_put(&mc, key, data, flags);
	*/
	return nil
}

func (t *transaction) Bucket(name string, flags int) (*Bucket, error) {
	/*
		MDB_val key, data;
		MDB_dbi i;
		MDB_cursor mc;
		int rc, dbflag, exact;
		unsigned int unused = 0;
		size_t len;

		if (txn->mt_dbxs[FREE_DBI].md_cmp == NULL) {
			mdb_default_cmp(txn, FREE_DBI);
		}

		if ((flags & VALID_FLAGS) != flags)
			return EINVAL;
		if (txn->mt_flags & MDB_TXN_ERROR)
			return MDB_BAD_TXN;

		// main DB?
		if (!name) {
			*dbi = MAIN_DBI;
			if (flags & PERSISTENT_FLAGS) {
				uint16_t f2 = flags & PERSISTENT_FLAGS;
				// make sure flag changes get committed
				if ((txn->mt_dbs[MAIN_DBI].md_flags | f2) != txn->mt_dbs[MAIN_DBI].md_flags) {
					txn->mt_dbs[MAIN_DBI].md_flags |= f2;
					txn->mt_flags |= MDB_TXN_DIRTY;
				}
			}
			mdb_default_cmp(txn, MAIN_DBI);
			return MDB_SUCCESS;
		}

		if (txn->mt_dbxs[MAIN_DBI].md_cmp == NULL) {
			mdb_default_cmp(txn, MAIN_DBI);
		}

		// Is the DB already open?
		len = strlen(name);
		for (i=2; i<txn->mt_numdbs; i++) {
			if (!txn->mt_dbxs[i].md_name.mv_size) {
				// Remember this free slot
				if (!unused) unused = i;
				continue;
			}
			if (len == txn->mt_dbxs[i].md_name.mv_size &&
				!strncmp(name, txn->mt_dbxs[i].md_name.mv_data, len)) {
				*dbi = i;
				return MDB_SUCCESS;
			}
		}

		// If no free slot and max hit, fail
		if (!unused && txn->mt_numdbs >= txn->mt_env->me_maxdbs)
			return MDB_DBS_FULL;

		// Cannot mix named databases with some mainDB flags
		if (txn->mt_dbs[MAIN_DBI].md_flags & (MDB_DUPSORT|MDB_INTEGERKEY))
			return (flags & MDB_CREATE) ? MDB_INCOMPATIBLE : MDB_NOTFOUND;

		// Find the DB info
		dbflag = DB_NEW|DB_VALID;
		exact = 0;
		key.mv_size = len;
		key.mv_data = (void *)name;
		mdb_cursor_init(&mc, txn, MAIN_DBI, NULL);
		rc = mdb_cursor_set(&mc, &key, &data, MDB_SET, &exact);
		if (rc == MDB_SUCCESS) {
			// make sure this is actually a DB
			MDB_node *node = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
			if (!(node->mn_flags & F_SUBDATA))
				return MDB_INCOMPATIBLE;
		} else if (rc == MDB_NOTFOUND && (flags & MDB_CREATE)) {
			// Create if requested
			MDB_db dummy;
			data.mv_size = sizeof(MDB_db);
			data.mv_data = &dummy;
			memset(&dummy, 0, sizeof(dummy));
			dummy.md_root = P_INVALID;
			dummy.md_flags = flags & PERSISTENT_FLAGS;
			rc = mdb_cursor_put(&mc, &key, &data, F_SUBDATA);
			dbflag |= DB_DIRTY;
		}

		// OK, got info, add to table
		if (rc == MDB_SUCCESS) {
			unsigned int slot = unused ? unused : txn->mt_numdbs;
			txn->mt_dbxs[slot].md_name.mv_data = strdup(name);
			txn->mt_dbxs[slot].md_name.mv_size = len;
			txn->mt_dbxs[slot].md_rel = NULL;
			txn->mt_dbflags[slot] = dbflag;
			memcpy(&txn->mt_dbs[slot], data.mv_data, sizeof(MDB_db));
			*dbi = slot;
			mdb_default_cmp(txn, slot);
			if (!unused) {
				txn->mt_numdbs++;
			}
		}

		return rc;
	*/
	return nil, nil
}

func (t *transaction) Stat(b Bucket) *stat {
	/*
		if (txn == NULL || arg == NULL || dbi >= txn->mt_numdbs)
			return EINVAL;

		if (txn->mt_dbflags[dbi] & DB_STALE) {
			MDB_cursor mc;
			MDB_xcursor mx;
			// Stale, must read the DB's root. cursor_init does it for us.
			mdb_cursor_init(&mc, txn, dbi, &mx);
		}
		return mdb_stat0(txn->mt_env, &txn->mt_dbs[dbi], arg);
	*/
	return nil
}

func (t *transaction) BucketFlags(b Bucket) (int, error) {
	/*
		// We could return the flags for the FREE_DBI too but what's the point?
		if (txn == NULL || dbi < MAIN_DBI || dbi >= txn->mt_numdbs)
			return EINVAL;
		*flags = txn->mt_dbs[dbi].md_flags & PERSISTENT_FLAGS;
		return MDB_SUCCESS;
	*/
	return 0, nil
}

func (t *transaction) Drop(b *Bucket, del int) error {
	/*
			MDB_cursor *mc, *m2;
			int rc;

			if (!txn || !dbi || dbi >= txn->mt_numdbs || (unsigned)del > 1 || !(txn->mt_dbflags[dbi] & DB_VALID))
				return EINVAL;

			if (F_ISSET(txn->mt_flags, MDB_TXN_RDONLY))
				return EACCES;

			rc = mdb_cursor_open(txn, dbi, &mc);
			if (rc)
				return rc;

			rc = mdb_drop0(mc, mc->mc_db->md_flags & MDB_DUPSORT);
			// Invalidate the dropped DB's cursors
			for (m2 = txn->mt_cursors[dbi]; m2; m2 = m2->mc_next)
				m2->mc_flags &= ~(C_INITIALIZED|C_EOF);
			if (rc)
				goto leave;

			// Can't delete the main DB
			if (del && dbi > MAIN_DBI) {
				rc = mdb_del(txn, MAIN_DBI, &mc->mc_dbx->md_name, NULL);
				if (!rc) {
					txn->mt_dbflags[dbi] = DB_STALE;
					mdb_dbi_close(txn->mt_env, dbi);
				}
			} else {
				// reset the DB record, mark it dirty 
				txn->mt_dbflags[dbi] |= DB_DIRTY;
				txn->mt_dbs[dbi].md_depth = 0;
				txn->mt_dbs[dbi].md_branch_pages = 0;
				txn->mt_dbs[dbi].md_leaf_pages = 0;
				txn->mt_dbs[dbi].md_overflow_pages = 0;
				txn->mt_dbs[dbi].md_entries = 0;
				txn->mt_dbs[dbi].md_root = P_INVALID;

				txn->mt_flags |= MDB_TXN_DIRTY;
			}
		leave:
			mdb_cursor_close(mc);
			return rc;
	*/
	return nil
}

package bolt

/*
type RWCursor interface {
	Put([]byte, []byte) (error)
	Delete([]byte) (error)
}
*/

// RWCursor represents a cursor that can read and write data for a bucket.
type RWCursor struct {
	Cursor
	transaction *RWTransaction
	reclaimed   []pgno /**< Reclaimed freeDB pages, or NULL before use (was me_pghead) */
	last        txnid  /**< ID of last used record, or 0 if len(reclaimed) == 0 */
}

func (c *RWCursor) Put(key []byte, value []byte) error {
	// Make sure this cursor was created by a transaction.
	if c.transaction == nil {
		return &Error{"invalid cursor", nil}
	}
	db := c.transaction.db

	// Validate the key we're using.
	if key == nil {
		return &Error{"key required", nil}
	} else if len(key) > db.maxKeySize {
		return &Error{"key too large", nil}
	}

	// TODO: Validate data size based on MaxKeySize if DUPSORT.

	// Validate the size of our data.
	if len(data) > MaxDataSize {
		return &Error{"data too large", nil}
	}

	// If we don't have a root page then add one.
	if c.bucket.root == p_invalid {
		p, err := c.newLeafPage()
		if err != nil {
			return err
		}
		c.push(p)
		c.bucket.root = p.id
		c.bucket.root++
		// TODO: *mc->mc_dbflag |= DB_DIRTY;
		// TODO? mc->mc_flags |= C_INITIALIZED;
	}

	// TODO: Move to key.
	exists, err := c.moveTo(key)
	if err != nil {
		return err
	}

	// TODO: spill?
	if err := c.spill(key, data); err != nil {
		return err
	}

	// Make sure all cursor pages are writable
	if err := c.touch(); err != nil {
		return err
	}

	// If key does not exist the
	if exists {
		node := c.currentNode()

	}

	/*

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
							dummy.md_pad = 0;
							dummy.md_flags = 0;
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

// newLeafPage allocates and initialize new a new leaf page.
func (c *RWCursor) newLeafPage() (*page, error) {
	// Allocate page.
	p, err := c.allocatePage(1)
	if err != nil {
		return nil, err
	}

	// Set flags and bounds.
	p.flags = p_leaf | p_dirty
	p.lower = pageHeaderSize
	p.upper = c.transaction.db.pageSize
	c.leafs += 1

	return p, nil
}

// newBranchPage allocates and initialize new a new branch page.
func (b *RWCursor) newBranchPage() (*page, error) {
	// Allocate page.
	p, err := c.allocatePage(1)
	if err != nil {
		return nil, err
	}

	// Set flags and bounds.
	p.flags = p_branch | p_dirty
	p.lower = pageHeaderSize
	p.upper = c.transaction.db.pageSize
	c.bucket.branches += 1

	return p, nil
}

// newOverflowPage allocates and initialize new overflow pages.
func (b *RWCursor) newOverflowPage(count int) (*page, error) {
	// Allocate page.
	p, err := c.allocatePage(count)
	if err != nil {
		return nil, err
	}

	// Set flags and bounds.
	p.flags = p_overflow | p_dirty
	p.lower = pageHeaderSize
	p.upper = c.transaction.db.pageSize
	c.bucket.overflows += count

	return p, nil
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

// allocatePage allocates a new page.
func (c *RWCursor) allocatePage(count int) (*page, error) {
	head := env.pagestate.head

	// TODO?
	// If our dirty list is already full, we can't do anything
	// if (txn->mt_dirty_room == 0) {
	//   rc = MDB_TXN_FULL;
	//   goto fail;
	// }

	/*
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
			if (!(np = mdb_page_malloc(txn, num))) {
				rc = ENOMEM;
				goto fail;
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
	return nil
}

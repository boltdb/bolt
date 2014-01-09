package bolt

const (
	NoSync = iota
	NoMetaSync
	DupSort
	IntegerKey
	IntegerDupKey
)

// TODO: #define	MDB_FATAL_ERROR	0x80000000U /** Failed to update the meta page. Probably an I/O error. */
// TODO: #define	MDB_ENV_ACTIVE	0x20000000U /** Some fields are initialized. */
// TODO: #define	MDB_ENV_TXKEY	0x10000000U /** me_txkey is set */
// TODO: #define	MDB_LIVE_READER	0x08000000U /** Have liveness lock in reader table */

// Only a subset of the @ref mdb_env flags can be changed
// at runtime. Changing other flags requires closing the
// environment and re-opening it with the new flags.
// TODO: #define	CHANGEABLE	(MDB_NOSYNC|MDB_NOMETASYNC|MDB_MAPASYNC|MDB_NOMEMINIT)
// TODO: #define	CHANGELESS	(MDB_FIXEDMAP|MDB_NOSUBDIR|MDB_RDONLY|MDB_WRITEMAP| MDB_NOTLS|MDB_NOLOCK|MDB_NORDAHEAD)

type DB interface {
}

type db struct {
	sync.Mutex
	file os.File
	flags int
	pageSize int
	osPageSize int
	readers []*reader
	buckets []*bucket
	xbuckets []*bucketx /**< array of static DB info */
	bucketFlags []int /**< array of flags from MDB_db.md_flags */
	path string
	mmap []byte
	mmapSize int /**< size of the data memory map */
	size int /**< current file size */
	meta1 []byte
	meta2 []byte
	pbuf []byte
	transaction *transaction /**< current write transaction */
	maxPageNumber int /**< me_mapsize / me_psize */
	pageState pageStage /**< state of old pages from freeDB */
	dpages []*page /**< list of malloc'd blocks for re-use */
	freePageNumbers []int /** IDL of pages that became unused in a write txn */
	dirtyPageNumbers []int /** ID2L of pages written during a write txn. Length MDB_IDL_UM_SIZE. */
	maxFreeOnePage int /** Max number of freelist items that can fit in a single overflow page */
	maxNodeSize int /** Max size of a node on a page */
	maxKeySize int /**< max size of a key */
}

func NewDB() DB {
	return &db{}
}

func (db *db) Path() string {
	return db.path
}

func (db *db) freePage(p *page) {
	/*
	mp->mp_next = env->me_dpages;
	VGMEMP_FREE(env, mp);
	env->me_dpages = mp;
	*/
}

func (db *db) freeDirtyPage(p *page) {
	/*
	if (!IS_OVERFLOW(dp) || dp->mp_pages == 1) {
		mdb_page_free(env, dp);
	} else {
		// large pages just get freed directly
		VGMEMP_FREE(env, dp);
		free(dp);
	}
	*/
}

func (db *db) freeAllDirtyPages(p *page) {
	/*
	MDB_env *env = txn->mt_env;
	MDB_ID2L dl = txn->mt_u.dirty_list;
	unsigned i, n = dl[0].mid;

	for (i = 1; i <= n; i++) {
		mdb_dpage_free(env, dl[i].mptr);
	}
	dl[0].mid = 0;
	*/
}

func (db *db) sync(force bool) error {
	/*
	int rc = 0;
	if (force || !F_ISSET(env->me_flags, MDB_NOSYNC)) {
		if (env->me_flags & MDB_WRITEMAP) {
			int flags = ((env->me_flags & MDB_MAPASYNC) && !force)
				? MS_ASYNC : MS_SYNC;
			if (MDB_MSYNC(env->me_map, env->me_mapsize, flags))
				rc = ErrCode();
#ifdef _WIN32
			else if (flags == MS_SYNC && MDB_FDATASYNC(env->me_fd))
				rc = ErrCode();
#endif
		} else {
			if (MDB_FDATASYNC(env->me_fd))
				rc = ErrCode();
		}
	}
	return rc;
	*/
	return nil
}

func (db *db) Transaction(parent *transaction, flags int) (*transaction, error) {
	/*
	MDB_txn *txn;
	MDB_ntxn *ntxn;
	int rc, size, tsize = sizeof(MDB_txn);

	if (env->me_flags & MDB_FATAL_ERROR) {
		DPUTS("environment had fatal error, must shutdown!");
		return MDB_PANIC;
	}
	if ((env->me_flags & MDB_RDONLY) && !(flags & MDB_RDONLY))
		return EACCES;
	if (parent) {
		// Nested transactions: Max 1 child, write txns only, no writemap
		if (parent->mt_child ||
			(flags & MDB_RDONLY) ||
			(parent->mt_flags & (MDB_TXN_RDONLY|MDB_TXN_ERROR)) ||
			(env->me_flags & MDB_WRITEMAP))
		{
			return (parent->mt_flags & MDB_TXN_RDONLY) ? EINVAL : MDB_BAD_TXN;
		}
		tsize = sizeof(MDB_ntxn);
	}
	size = tsize + env->me_maxdbs * (sizeof(MDB_db)+1);
	if (!(flags & MDB_RDONLY))
		size += env->me_maxdbs * sizeof(MDB_cursor *);

	if ((txn = calloc(1, size)) == NULL) {
		DPRINTF(("calloc: %s", strerror(ErrCode())));
		return ENOMEM;
	}
	txn->mt_dbs = (MDB_db *) ((char *)txn + tsize);
	if (flags & MDB_RDONLY) {
		txn->mt_flags |= MDB_TXN_RDONLY;
		txn->mt_dbflags = (unsigned char *)(txn->mt_dbs + env->me_maxdbs);
	} else {
		txn->mt_cursors = (MDB_cursor **)(txn->mt_dbs + env->me_maxdbs);
		txn->mt_dbflags = (unsigned char *)(txn->mt_cursors + env->me_maxdbs);
	}
	txn->mt_env = env;

	if (parent) {
		unsigned int i;
		txn->mt_u.dirty_list = malloc(sizeof(MDB_ID2)*MDB_IDL_UM_SIZE);
		if (!txn->mt_u.dirty_list ||
			!(txn->mt_free_pgs = mdb_midl_alloc(MDB_IDL_UM_MAX)))
		{
			free(txn->mt_u.dirty_list);
			free(txn);
			return ENOMEM;
		}
		txn->mt_txnid = parent->mt_txnid;
		txn->mt_dirty_room = parent->mt_dirty_room;
		txn->mt_u.dirty_list[0].mid = 0;
		txn->mt_spill_pgs = NULL;
		txn->mt_next_pgno = parent->mt_next_pgno;
		parent->mt_child = txn;
		txn->mt_parent = parent;
		txn->mt_numdbs = parent->mt_numdbs;
		txn->mt_flags = parent->mt_flags;
		txn->mt_dbxs = parent->mt_dbxs;
		memcpy(txn->mt_dbs, parent->mt_dbs, txn->mt_numdbs * sizeof(MDB_db));
		// Copy parent's mt_dbflags, but clear DB_NEW
		for (i=0; i<txn->mt_numdbs; i++)
			txn->mt_dbflags[i] = parent->mt_dbflags[i] & ~DB_NEW;
		rc = 0;
		ntxn = (MDB_ntxn *)txn;
		ntxn->mnt_pgstate = env->me_pgstate; // save parent me_pghead & co
		if (env->me_pghead) {
			size = MDB_IDL_SIZEOF(env->me_pghead);
			env->me_pghead = mdb_midl_alloc(env->me_pghead[0]);
			if (env->me_pghead)
				memcpy(env->me_pghead, ntxn->mnt_pgstate.mf_pghead, size);
			else
				rc = ENOMEM;
		}
		if (!rc)
			rc = mdb_cursor_shadow(parent, txn);
		if (rc)
			mdb_txn_reset0(txn, "beginchild-fail");
	} else {
		rc = mdb_txn_renew0(txn);
	}
	if (rc)
		free(txn);
	else {
		*ret = txn;
		DPRINTF(("begin txn %"Z"u%c %p on mdbenv %p, root page %"Z"u",
			txn->mt_txnid, (txn->mt_flags & MDB_TXN_RDONLY) ? 'r' : 'w',
			(void *) txn, (void *) env, txn->mt_dbs[MAIN_DBI].md_root));
	}

	return rc;
	*/
	return nil
}

// Read the environment parameters of a DB environment before
// mapping it into memory.
// @param[in] env the environment handle
// @param[out] meta address of where to store the meta information
// @return 0 on success, non-zero on failure.
func (db *db) readHeader(meta *meta) error {
	/*
	MDB_metabuf	pbuf;
	MDB_page	*p;
	MDB_meta	*m;
	int			i, rc, off;
	enum { Size = sizeof(pbuf) };

	// We don't know the page size yet, so use a minimum value.
	// Read both meta pages so we can use the latest one.

	for (i=off=0; i<2; i++, off = meta->mm_psize) {
#ifdef _WIN32
		DWORD len;
		OVERLAPPED ov;
		memset(&ov, 0, sizeof(ov));
		ov.Offset = off;
		rc = ReadFile(env->me_fd, &pbuf, Size, &len, &ov) ? (int)len : -1;
		if (rc == -1 && ErrCode() == ERROR_HANDLE_EOF)
			rc = 0;
#else
		rc = pread(env->me_fd, &pbuf, Size, off);
#endif
		if (rc != Size) {
			if (rc == 0 && off == 0)
				return ENOENT;
			rc = rc < 0 ? (int) ErrCode() : MDB_INVALID;
			DPRINTF(("read: %s", mdb_strerror(rc)));
			return rc;
		}

		p = (MDB_page *)&pbuf;

		if (!F_ISSET(p->mp_flags, P_META)) {
			DPRINTF(("page %"Z"u not a meta page", p->mp_pgno));
			return MDB_INVALID;
		}

		m = METADATA(p);
		if (m->mm_magic != MDB_MAGIC) {
			DPUTS("meta has invalid magic");
			return MDB_INVALID;
		}

		if (m->mm_version != MDB_DATA_VERSION) {
			DPRINTF(("database is version %u, expected version %u",
				m->mm_version, MDB_DATA_VERSION));
			return MDB_VERSION_MISMATCH;
		}

		if (off == 0 || m->mm_txnid > meta->mm_txnid)
			*meta = *m;
	}
	return 0;
	*/
	return nil
}

// Write the environment parameters of a freshly created DB environment.
// @param[in] env the environment handle
// @param[out] meta address of where to store the meta information
// @return 0 on success, non-zero on failure.
func (db *db) initMeta(meta *meta) error {
	/*
	MDB_page *p, *q;
	int rc;
	unsigned int	 psize;
#ifdef _WIN32
	DWORD len;
	OVERLAPPED ov;
	memset(&ov, 0, sizeof(ov));
#define DO_PWRITE(rc, fd, ptr, size, len, pos)	do { \
	ov.Offset = pos;	\
	rc = WriteFile(fd, ptr, size, &len, &ov);	} while(0)
#else
	int len;
#define DO_PWRITE(rc, fd, ptr, size, len, pos)	do { \
	len = pwrite(fd, ptr, size, pos);	\
	rc = (len >= 0); } while(0)
#endif

	DPUTS("writing new meta page");

	psize = env->me_psize;

	meta->mm_magic = MDB_MAGIC;
	meta->mm_version = MDB_DATA_VERSION;
	meta->mm_mapsize = env->me_mapsize;
	meta->mm_psize = psize;
	meta->mm_last_pg = 1;
	meta->mm_flags = env->me_flags & 0xffff;
	meta->mm_flags |= MDB_INTEGERKEY;
	meta->mm_dbs[0].md_root = P_INVALID;
	meta->mm_dbs[1].md_root = P_INVALID;

	p = calloc(2, psize);
	p->mp_pgno = 0;
	p->mp_flags = P_META;
	*(MDB_meta *)METADATA(p) = *meta;

	q = (MDB_page *)((char *)p + psize);
	q->mp_pgno = 1;
	q->mp_flags = P_META;
	*(MDB_meta *)METADATA(q) = *meta;

	DO_PWRITE(rc, env->me_fd, p, psize * 2, len, 0);
	if (!rc)
		rc = ErrCode();
	else if ((unsigned) len == psize * 2)
		rc = MDB_SUCCESS;
	else
		rc = ENOSPC;
	free(p);
	return rc;
	*/
	return nil
}

// Check both meta pages to see which one is newer.
// @param[in] env the environment handle
// @return meta toggle (0 or 1).
func (db *db) pickMeta() int {
	/*
	return (env->me_metas[0]->mm_txnid < env->me_metas[1]->mm_txnid);
	*/
	return 0
}

func (db *db) Create() error {
	/*
	MDB_env *e;

	e = calloc(1, sizeof(MDB_env));
	if (!e)
		return ENOMEM;

	e->me_maxreaders = DEFAULT_READERS;
	e->me_maxdbs = e->me_numdbs = 2;
	e->me_fd = INVALID_HANDLE_VALUE;
	e->me_lfd = INVALID_HANDLE_VALUE;
	e->me_mfd = INVALID_HANDLE_VALUE;
#ifdef MDB_USE_POSIX_SEM
	e->me_rmutex = SEM_FAILED;
	e->me_wmutex = SEM_FAILED;
#endif
	e->me_pid = getpid();
	GET_PAGESIZE(e->me_os_psize);
	VGMEMP_CREATE(e,0,0);
	*env = e;
	return MDB_SUCCESS;
	*/
	return nil
}

// int mdb_env_map(MDB_env *env, void *addr, int newsize)
func (db *db) openMmap(newsize int) {
	/*
	MDB_page *p;
	unsigned int flags = env->me_flags;
#ifdef _WIN32
	int rc;
	HANDLE mh;
	LONG sizelo, sizehi;
	sizelo = env->me_mapsize & 0xffffffff;
	sizehi = env->me_mapsize >> 16 >> 16; // only needed on Win64

	// Windows won't create mappings for zero length files.
	// Just allocate the maxsize right now.
	if (newsize) {
		if (SetFilePointer(env->me_fd, sizelo, &sizehi, 0) != (DWORD)sizelo
			|| !SetEndOfFile(env->me_fd)
			|| SetFilePointer(env->me_fd, 0, NULL, 0) != 0)
			return ErrCode();
	}
	mh = CreateFileMapping(env->me_fd, NULL, flags & MDB_WRITEMAP ?
		PAGE_READWRITE : PAGE_READONLY,
		sizehi, sizelo, NULL);
	if (!mh)
		return ErrCode();
	env->me_map = MapViewOfFileEx(mh, flags & MDB_WRITEMAP ?
		FILE_MAP_WRITE : FILE_MAP_READ,
		0, 0, env->me_mapsize, addr);
	rc = env->me_map ? 0 : ErrCode();
	CloseHandle(mh);
	if (rc)
		return rc;
#else
	int prot = PROT_READ;
	if (flags & MDB_WRITEMAP) {
		prot |= PROT_WRITE;
		if (ftruncate(env->me_fd, env->me_mapsize) < 0)
			return ErrCode();
	}
	env->me_map = mmap(addr, env->me_mapsize, prot, MAP_SHARED,
		env->me_fd, 0);
	if (env->me_map == MAP_FAILED) {
		env->me_map = NULL;
		return ErrCode();
	}

	if (flags & MDB_NORDAHEAD) {
		// Turn off readahead. It's harmful when the DB is larger than RAM.
#ifdef MADV_RANDOM
		madvise(env->me_map, env->me_mapsize, MADV_RANDOM);
#else
#ifdef POSIX_MADV_RANDOM
		posix_madvise(env->me_map, env->me_mapsize, POSIX_MADV_RANDOM);
#endif // POSIX_MADV_RANDOM
#endif // MADV_RANDOM
	}
#endif // _WIN32

	// Can happen because the address argument to mmap() is just a
	// hint.  mmap() can pick another, e.g. if the range is in use.
	// The MAP_FIXED flag would prevent that, but then mmap could
	// instead unmap existing pages to make room for the new map.
	if (addr && env->me_map != addr)
		return EBUSY;	// TODO: Make a new MDB_* error code?

	p = (MDB_page *)env->me_map;
	env->me_metas[0] = METADATA(p);
	env->me_metas[1] = (MDB_meta *)((char *)env->me_metas[0] + env->me_psize);

	return MDB_SUCCESS;
	*/
	return nil
}

func (db *db) setMapSize(size int) error {
	/*
	// If env is already open, caller is responsible for making
	// sure there are no active txns.
	if (env->me_map) {
		int rc;
		void *old;
		if (env->me_txn)
			return EINVAL;
		if (!size)
			size = env->me_metas[mdb_env_pick_meta(env)]->mm_mapsize;
		else if (size < env->me_mapsize) {
			// If the configured size is smaller, make sure it's
			// still big enough. Silently round up to minimum if not.
			size_t minsize = (env->me_metas[mdb_env_pick_meta(env)]->mm_last_pg + 1) * env->me_psize;
			if (size < minsize)
				size = minsize;
		}
		munmap(env->me_map, env->me_mapsize);
		env->me_mapsize = size;
		old = (env->me_flags & MDB_FIXEDMAP) ? env->me_map : NULL;
		rc = mdb_env_map(env, old, 1);
		if (rc)
			return rc;
	}
	env->me_mapsize = size;
	if (env->me_psize)
		env->me_maxpg = env->me_mapsize / env->me_psize;
	return MDB_SUCCESS;
	*/
	return nil
}

func (db *db) setMaxBucketCount(count int) error {
	/*
	if (env->me_map)
		return EINVAL;
	env->me_maxdbs = dbs + 2; // Named databases + main and free DB
	return MDB_SUCCESS;
	*/
	return nil
}

func (db *db) setMaxReaderCount(count int) error {
	/*
	if (env->me_map || readers < 1)
		return EINVAL;
	env->me_maxreaders = readers;
	return MDB_SUCCESS;
	*/
}

func (db *db) getMaxReaderCount(count int) (int, error) {
	/*
	if (!env || !readers)
		return EINVAL;
	*readers = env->me_maxreaders;
	return MDB_SUCCESS;
	*/
	return 0, nil
}

// Further setup required for opening an MDB environment
func (db *db) open() error {
	/*
	unsigned int flags = env->me_flags;
	int i, newenv = 0, rc;
	MDB_meta meta;

#ifdef _WIN32
	// See if we should use QueryLimited
	rc = GetVersion();
	if ((rc & 0xff) > 5)
		env->me_pidquery = MDB_PROCESS_QUERY_LIMITED_INFORMATION;
	else
		env->me_pidquery = PROCESS_QUERY_INFORMATION;
#endif // _WIN32

	memset(&meta, 0, sizeof(meta));

	if ((i = mdb_env_read_header(env, &meta)) != 0) {
		if (i != ENOENT)
			return i;
		DPUTS("new mdbenv");
		newenv = 1;
		env->me_psize = env->me_os_psize;
		if (env->me_psize > MAX_PAGESIZE)
			env->me_psize = MAX_PAGESIZE;
	} else {
		env->me_psize = meta.mm_psize;
	}

	// Was a mapsize configured?
	if (!env->me_mapsize) {
		// If this is a new environment, take the default,
		// else use the size recorded in the existing env.
		env->me_mapsize = newenv ? DEFAULT_MAPSIZE : meta.mm_mapsize;
	} else if (env->me_mapsize < meta.mm_mapsize) {
		// If the configured size is smaller, make sure it's
		// still big enough. Silently round up to minimum if not.
		size_t minsize = (meta.mm_last_pg + 1) * meta.mm_psize;
		if (env->me_mapsize < minsize)
			env->me_mapsize = minsize;
	}

	rc = mdb_env_map(env, meta.mm_address, newenv);
	if (rc)
		return rc;

	if (newenv) {
		if (flags & MDB_FIXEDMAP)
			meta.mm_address = env->me_map;
		i = mdb_env_init_meta(env, &meta);
		if (i != MDB_SUCCESS) {
			return i;
		}
	}

	env->me_maxfree_1pg = (env->me_psize - PAGEHDRSZ) / sizeof(pgno_t) - 1;
	env->me_nodemax = (((env->me_psize - PAGEHDRSZ) / MDB_MINKEYS) & -2)
		- sizeof(indx_t);
#if !(MDB_MAXKEYSIZE)
	env->me_maxkey = env->me_nodemax - (NODESIZE + sizeof(MDB_db));
#endif
	env->me_maxpg = env->me_mapsize / env->me_psize;

#if MDB_DEBUG
	{
		int toggle = mdb_env_pick_meta(env);
		MDB_db *db = &env->me_metas[toggle]->mm_dbs[MAIN_DBI];

		DPRINTF(("opened database version %u, pagesize %u",
			env->me_metas[0]->mm_version, env->me_psize));
		DPRINTF(("using meta page %d",    toggle));
		DPRINTF(("depth: %u",             db->md_depth));
		DPRINTF(("entries: %"Z"u",        db->md_entries));
		DPRINTF(("branch pages: %"Z"u",   db->md_branch_pages));
		DPRINTF(("leaf pages: %"Z"u",     db->md_leaf_pages));
		DPRINTF(("overflow pages: %"Z"u", db->md_overflow_pages));
		DPRINTF(("root: %"Z"u",           db->md_root));
	}
#endif

	return MDB_SUCCESS;
	*/
	return nil
}

func (db *db) Open(path string, flags int, mode uint) error {
	/*
	int		oflags, rc, len, excl = -1;
	char *lpath, *dpath;

	if (env->me_fd!=INVALID_HANDLE_VALUE || (flags & ~(CHANGEABLE|CHANGELESS)))
		return EINVAL;

	len = strlen(path);
	if (flags & MDB_NOSUBDIR) {
		rc = len + sizeof(LOCKSUFF) + len + 1;
	} else {
		rc = len + sizeof(LOCKNAME) + len + sizeof(DATANAME);
	}
	lpath = malloc(rc);
	if (!lpath)
		return ENOMEM;
	if (flags & MDB_NOSUBDIR) {
		dpath = lpath + len + sizeof(LOCKSUFF);
		sprintf(lpath, "%s" LOCKSUFF, path);
		strcpy(dpath, path);
	} else {
		dpath = lpath + len + sizeof(LOCKNAME);
		sprintf(lpath, "%s" LOCKNAME, path);
		sprintf(dpath, "%s" DATANAME, path);
	}

	rc = MDB_SUCCESS;
	flags |= env->me_flags;
	if (flags & MDB_RDONLY) {
		// silently ignore WRITEMAP when we're only getting read access
		flags &= ~MDB_WRITEMAP;
	} else {
		if (!((env->me_free_pgs = mdb_midl_alloc(MDB_IDL_UM_MAX)) &&
			  (env->me_dirty_list = calloc(MDB_IDL_UM_SIZE, sizeof(MDB_ID2)))))
			rc = ENOMEM;
	}
	env->me_flags = flags |= MDB_ENV_ACTIVE;
	if (rc)
		goto leave;

	env->me_path = strdup(path);
	env->me_dbxs = calloc(env->me_maxdbs, sizeof(MDB_dbx));
	env->me_dbflags = calloc(env->me_maxdbs, sizeof(uint16_t));
	if (!(env->me_dbxs && env->me_path && env->me_dbflags)) {
		rc = ENOMEM;
		goto leave;
	}

	// For RDONLY, get lockfile after we know datafile exists
	if (!(flags & (MDB_RDONLY|MDB_NOLOCK))) {
		rc = mdb_env_setup_locks(env, lpath, mode, &excl);
		if (rc)
			goto leave;
	}

#ifdef _WIN32
	if (F_ISSET(flags, MDB_RDONLY)) {
		oflags = GENERIC_READ;
		len = OPEN_EXISTING;
	} else {
		oflags = GENERIC_READ|GENERIC_WRITE;
		len = OPEN_ALWAYS;
	}
	mode = FILE_ATTRIBUTE_NORMAL;
	env->me_fd = CreateFile(dpath, oflags, FILE_SHARE_READ|FILE_SHARE_WRITE,
		NULL, len, mode, NULL);
#else
	if (F_ISSET(flags, MDB_RDONLY))
		oflags = O_RDONLY;
	else
		oflags = O_RDWR | O_CREAT;

	env->me_fd = open(dpath, oflags, mode);
#endif
	if (env->me_fd == INVALID_HANDLE_VALUE) {
		rc = ErrCode();
		goto leave;
	}

	if ((flags & (MDB_RDONLY|MDB_NOLOCK)) == MDB_RDONLY) {
		rc = mdb_env_setup_locks(env, lpath, mode, &excl);
		if (rc)
			goto leave;
	}

	if ((rc = mdb_env_open2(env)) == MDB_SUCCESS) {
		if (flags & (MDB_RDONLY|MDB_WRITEMAP)) {
			env->me_mfd = env->me_fd;
		} else {
			// Synchronous fd for meta writes. Needed even with
			// MDB_NOSYNC/MDB_NOMETASYNC, in case these get reset.
#ifdef _WIN32
			len = OPEN_EXISTING;
			env->me_mfd = CreateFile(dpath, oflags,
				FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, len,
				mode | FILE_FLAG_WRITE_THROUGH, NULL);
#else
			oflags &= ~O_CREAT;
			env->me_mfd = open(dpath, oflags | MDB_DSYNC, mode);
#endif
			if (env->me_mfd == INVALID_HANDLE_VALUE) {
				rc = ErrCode();
				goto leave;
			}
		}
		DPRINTF(("opened dbenv %p", (void *) env));
		if (excl > 0) {
			rc = mdb_env_share_locks(env, &excl);
			if (rc)
				goto leave;
		}
		if (!((flags & MDB_RDONLY) ||
			  (env->me_pbuf = calloc(1, env->me_psize))))
			rc = ENOMEM;
	}

leave:
	if (rc) {
		mdb_env_close0(env, excl);
	}
	free(lpath);
	return rc;
	*/
	return nil
}

// Destroy resources from mdb_env_open(), clear our readers & DBIs
func (db *db) close0(excl) {
	/*
	int i;

	if (!(env->me_flags & MDB_ENV_ACTIVE))
		return;

	// Doing this here since me_dbxs may not exist during mdb_env_close
	for (i = env->me_maxdbs; --i > MAIN_DBI; )
		free(env->me_dbxs[i].md_name.mv_data);

	free(env->me_pbuf);
	free(env->me_dbflags);
	free(env->me_dbxs);
	free(env->me_path);
	free(env->me_dirty_list);
	mdb_midl_free(env->me_free_pgs);

	if (env->me_flags & MDB_ENV_TXKEY) {
		pthread_key_delete(env->me_txkey);
#ifdef _WIN32
		// Delete our key from the global list
		for (i=0; i<mdb_tls_nkeys; i++)
			if (mdb_tls_keys[i] == env->me_txkey) {
				mdb_tls_keys[i] = mdb_tls_keys[mdb_tls_nkeys-1];
				mdb_tls_nkeys--;
				break;
			}
#endif
	}

	if (env->me_map) {
		munmap(env->me_map, env->me_mapsize);
	}
	if (env->me_mfd != env->me_fd && env->me_mfd != INVALID_HANDLE_VALUE)
		(void) close(env->me_mfd);
	if (env->me_fd != INVALID_HANDLE_VALUE)
		(void) close(env->me_fd);
	if (env->me_txns) {
		MDB_PID_T pid = env->me_pid;
		// Clearing readers is done in this function because
		// me_txkey with its destructor must be disabled first.
		for (i = env->me_numreaders; --i >= 0; )
			if (env->me_txns->mti_readers[i].mr_pid == pid)
				env->me_txns->mti_readers[i].mr_pid = 0;
#ifdef _WIN32
		if (env->me_rmutex) {
			CloseHandle(env->me_rmutex);
			if (env->me_wmutex) CloseHandle(env->me_wmutex);
		}
		// Windows automatically destroys the mutexes when
		// the last handle closes.
#elif defined(MDB_USE_POSIX_SEM)
		if (env->me_rmutex != SEM_FAILED) {
			sem_close(env->me_rmutex);
			if (env->me_wmutex != SEM_FAILED)
				sem_close(env->me_wmutex);
			// If we have the filelock:  If we are the
			// only remaining user, clean up semaphores.
			if (excl == 0)
				mdb_env_excl_lock(env, &excl);
			if (excl > 0) {
				sem_unlink(env->me_txns->mti_rmname);
				sem_unlink(env->me_txns->mti_wmname);
			}
		}
#endif
		munmap((void *)env->me_txns, (env->me_maxreaders-1)*sizeof(MDB_reader)+sizeof(MDB_txninfo));
	}
	if (env->me_lfd != INVALID_HANDLE_VALUE) {
#ifdef _WIN32
		if (excl >= 0) {
			// Unlock the lockfile.  Windows would have unlocked it
			// after closing anyway, but not necessarily at once.
			UnlockFile(env->me_lfd, 0, 0, 1, 0);
		}
#endif
		(void) close(env->me_lfd);
	}

	env->me_flags &= ~(MDB_ENV_ACTIVE|MDB_ENV_TXKEY);
	*/
}

func (db *db) copyfd(handle int) error {
	/*
	MDB_txn *txn = NULL;
	int rc;
	size_t wsize;
	char *ptr;
#ifdef _WIN32
	DWORD len, w2;
#define DO_WRITE(rc, fd, ptr, w2, len)	rc = WriteFile(fd, ptr, w2, &len, NULL)
#else
	ssize_t len;
	size_t w2;
#define DO_WRITE(rc, fd, ptr, w2, len)	len = write(fd, ptr, w2); rc = (len >= 0)
#endif

	// Do the lock/unlock of the reader mutex before starting the
	// write txn.  Otherwise other read txns could block writers.
	rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	if (rc)
		return rc;

	if (env->me_txns) {
		// We must start the actual read txn after blocking writers
		mdb_txn_reset0(txn, "reset-stage1");

		// Temporarily block writers until we snapshot the meta pages
		LOCK_MUTEX_W(env);

		rc = mdb_txn_renew0(txn);
		if (rc) {
			UNLOCK_MUTEX_W(env);
			goto leave;
		}
	}

	wsize = env->me_psize * 2;
	ptr = env->me_map;
	w2 = wsize;
	while (w2 > 0) {
		DO_WRITE(rc, fd, ptr, w2, len);
		if (!rc) {
			rc = ErrCode();
			break;
		} else if (len > 0) {
			rc = MDB_SUCCESS;
			ptr += len;
			w2 -= len;
			continue;
		} else {
			// Non-blocking or async handles are not supported
			rc = EIO;
			break;
		}
	}
	if (env->me_txns)
		UNLOCK_MUTEX_W(env);

	if (rc)
		goto leave;

	wsize = txn->mt_next_pgno * env->me_psize - wsize;
	while (wsize > 0) {
		if (wsize > MAX_WRITE)
			w2 = MAX_WRITE;
		else
			w2 = wsize;
		DO_WRITE(rc, fd, ptr, w2, len);
		if (!rc) {
			rc = ErrCode();
			break;
		} else if (len > 0) {
			rc = MDB_SUCCESS;
			ptr += len;
			wsize -= len;
			continue;
		} else {
			rc = EIO;
			break;
		}
	}

leave:
	mdb_txn_abort(txn);
	return rc;
}

int
mdb_env_copy(MDB_env *env, const char *path)
{
	int rc, len;
	char *lpath;
	HANDLE newfd = INVALID_HANDLE_VALUE;

	if (env->me_flags & MDB_NOSUBDIR) {
		lpath = (char *)path;
	} else {
		len = strlen(path);
		len += sizeof(DATANAME);
		lpath = malloc(len);
		if (!lpath)
			return ENOMEM;
		sprintf(lpath, "%s" DATANAME, path);
	}

	// The destination path must exist, but the destination file must not.
	// We don't want the OS to cache the writes, since the source data is
	// already in the OS cache.
#ifdef _WIN32
	newfd = CreateFile(lpath, GENERIC_WRITE, 0, NULL, CREATE_NEW,
				FILE_FLAG_NO_BUFFERING|FILE_FLAG_WRITE_THROUGH, NULL);
#else
	newfd = open(lpath, O_WRONLY|O_CREAT|O_EXCL, 0666);
#endif
	if (newfd == INVALID_HANDLE_VALUE) {
		rc = ErrCode();
		goto leave;
	}

#ifdef O_DIRECT
	// Set O_DIRECT if the file system supports it
	if ((rc = fcntl(newfd, F_GETFL)) != -1)
		(void) fcntl(newfd, F_SETFL, rc | O_DIRECT);
#endif
#ifdef F_NOCACHE	// __APPLE__
	rc = fcntl(newfd, F_NOCACHE, 1);
	if (rc) {
		rc = ErrCode();
		goto leave;
	}
#endif

	rc = mdb_env_copyfd(env, newfd);

leave:
	if (!(env->me_flags & MDB_NOSUBDIR))
		free(lpath);
	if (newfd != INVALID_HANDLE_VALUE)
		if (close(newfd) < 0 && rc == MDB_SUCCESS)
			rc = ErrCode();

	return rc;
	*/
	return nil
}

func (db *db) Close() {
	/*
	MDB_page *dp;

	if (env == NULL)
		return;

	VGMEMP_DESTROY(env);
	while ((dp = env->me_dpages) != NULL) {
		VGMEMP_DEFINED(&dp->mp_next, sizeof(dp->mp_next));
		env->me_dpages = dp->mp_next;
		free(dp);
	}

	mdb_env_close0(env, 0);
	free(env);
	*/
}

// Calculate the size of a leaf node.
// The size depends on the environment's page size; if a data item
// is too large it will be put onto an overflow page and the node
// size will only include the key and not the data. Sizes are always
// rounded up to an even number of bytes, to guarantee 2-byte alignment
// of the #MDB_node headers.
// @param[in] env The environment handle.
// @param[in] key The key for the node.
// @param[in] data The data for the node.
// @return The number of bytes needed to store the node.
func (db *db) LeafSize(key []byte, data []byte) int {
	/*
	size_t		 sz;

	sz = LEAFSIZE(key, data);
	if (sz > env->me_nodemax) {
		// put on overflow page
		sz -= data->mv_size - sizeof(pgno_t);
	}

	return EVEN(sz + sizeof(indx_t));
	*/
	return 0
}

// Calculate the size of a branch node.
// The size should depend on the environment's page size but since
// we currently don't support spilling large keys onto overflow
// pages, it's simply the size of the #MDB_node header plus the
// size of the key. Sizes are always rounded up to an even number
// of bytes, to guarantee 2-byte alignment of the #MDB_node headers.
// @param[in] env The environment handle.
// @param[in] key The key for the node.
// @return The number of bytes needed to store the node.
func (db *db) BranchSize(key []byte) int {
	/*
	size_t		 sz;

	sz = INDXSIZE(key);
	if (sz > env->me_nodemax) {
		// put on overflow page
		// not implemented
		// sz -= key->size - sizeof(pgno_t);
	}

	return sz + sizeof(indx_t);
	*/
	return 0
}

func (db *db) SetFlags(flag int, onoff bool) error {
	/*
	if ((flag & CHANGEABLE) != flag)
		return EINVAL;
	if (onoff)
		env->me_flags |= flag;
	else
		env->me_flags &= ~flag;
	return MDB_SUCCESS;
	*/
	return nil
}

func (db *db) Flags() int {
	return db.flags
}


func (db *db) Stat() *Stat
	/*
	int toggle;

	if (env == NULL || arg == NULL)
		return EINVAL;

	toggle = mdb_env_pick_meta(env);
	stat := &Stat{}
	stat->ms_psize = env->me_psize;
	stat->ms_depth = db->md_depth;
	stat->ms_branch_pages = db->md_branch_pages;
	stat->ms_leaf_pages = db->md_leaf_pages;
	stat->ms_overflow_pages = db->md_overflow_pages;
	stat->ms_entries = db->md_entries;

	//return mdb_stat0(env, &env->me_metas[toggle]->mm_dbs[MAIN_DBI], stat);
	return stat
	*/
}

func (db *db) Info() *Info {
	/*
	int toggle;

	if (env == NULL || arg == NULL)
		return EINVAL;

	toggle = mdb_env_pick_meta(env);
	arg->me_mapaddr = (env->me_flags & MDB_FIXEDMAP) ? env->me_map : 0;
	arg->me_mapsize = env->me_mapsize;
	arg->me_maxreaders = env->me_maxreaders;

	// me_numreaders may be zero if this process never used any readers. Use
	// the shared numreader count if it exists.
	arg->me_numreaders = env->me_txns ? env->me_txns->mti_numreaders : env->me_numreaders;

	arg->me_last_pgno = env->me_metas[toggle]->mm_last_pg;
	arg->me_last_txnid = env->me_metas[toggle]->mm_txnid;
	return MDB_SUCCESS;
	*/
	return nil
}

// TODO: Move to bucket.go
func (db *db) CloseBucket(b Bucket) {
	/*
	char *ptr;
	if (dbi <= MAIN_DBI || dbi >= env->me_maxdbs)
		return;
	ptr = env->me_dbxs[dbi].md_name.mv_data;
	env->me_dbxs[dbi].md_name.mv_data = NULL;
	env->me_dbxs[dbi].md_name.mv_size = 0;
	env->me_dbflags[dbi] = 0;
	free(ptr);
	*/
}

//int mdb_reader_list(MDB_env *env, MDB_msg_func *func, void *ctx)
func (db *db) getReaderList() error {
	/*
	unsigned int i, rdrs;
	MDB_reader *mr;
	char buf[64];
	int rc = 0, first = 1;

	if (!env || !func)
		return -1;
	if (!env->me_txns) {
		return func("(no reader locks)\n", ctx);
	}
	rdrs = env->me_txns->mti_numreaders;
	mr = env->me_txns->mti_readers;
	for (i=0; i<rdrs; i++) {
		if (mr[i].mr_pid) {
			txnid_t	txnid = mr[i].mr_txnid;
			sprintf(buf, txnid == (txnid_t)-1 ?
				"%10d %"Z"x -\n" : "%10d %"Z"x %"Z"u\n",
				(int)mr[i].mr_pid, (size_t)mr[i].mr_tid, txnid);
			if (first) {
				first = 0;
				rc = func("    pid     thread     txnid\n", ctx);
				if (rc < 0)
					break;
			}
			rc = func(buf, ctx);
			if (rc < 0)
				break;
		}
	}
	if (first) {
		rc = func("(no active readers)\n", ctx);
	}
	return rc;
	*/
	return nil
}

// (bool return is whether reader is dead)
func (db *db) checkReaders() (bool, error) {
	/*
	unsigned int i, j, rdrs;
	MDB_reader *mr;
	MDB_PID_T *pids, pid;
	int count = 0;

	if (!env)
		return EINVAL;
	if (dead)
		*dead = 0;
	if (!env->me_txns)
		return MDB_SUCCESS;
	rdrs = env->me_txns->mti_numreaders;
	pids = malloc((rdrs+1) * sizeof(MDB_PID_T));
	if (!pids)
		return ENOMEM;
	pids[0] = 0;
	mr = env->me_txns->mti_readers;
	for (i=0; i<rdrs; i++) {
		if (mr[i].mr_pid && mr[i].mr_pid != env->me_pid) {
			pid = mr[i].mr_pid;
			if (mdb_pid_insert(pids, pid) == 0) {
				if (!mdb_reader_pid(env, Pidcheck, pid)) {
					LOCK_MUTEX_R(env);
					// Recheck, a new process may have reused pid
					if (!mdb_reader_pid(env, Pidcheck, pid)) {
						for (j=i; j<rdrs; j++)
							if (mr[j].mr_pid == pid) {
								DPRINTF(("clear stale reader pid %u txn %"Z"d",
									(unsigned) pid, mr[j].mr_txnid));
								mr[j].mr_pid = 0;
								count++;
							}
					}
					UNLOCK_MUTEX_R(env);
				}
			}
		}
	}
	free(pids);
	if (dead)
		*dead = count;
	return MDB_SUCCESS;
	*/
	return false, nil
}

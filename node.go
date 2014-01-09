package bolt

const (
	bigNode = 0x01
	subNode = 0x02
	dupNode = 0x04
)

type node struct {
	lo      int
	hi      int
	flags   int
	keySize int
	data    []byte
}

func (n *node) setFlags(f int) {
	// Valid flags: (F_DUPDATA|F_SUBDATA|MDB_RESERVE|MDB_APPEND)
	// TODO
}

func (n *node) size() int {
	return 0 // TODO: offsetof(MDB_node, mn_data)
}

// TODO: #define INDXSIZE(k)	 (NODESIZE + ((k) == NULL ? 0 : (k)->mv_size))
// TODO: #define LEAFSIZE(k, d)	 (NODESIZE + (k)->mv_size + (d)->mv_size)
// TODO: #define NODEPTR(p, i)	 ((MDB_node *)((char *)(p) + (p)->mp_ptrs[i]))
// TODO: #define NODEKEY(node)	 (void *)((node)->mn_data)
// TODO: #define NODEDATA(node)	 (void *)((char *)(node)->mn_data + (node)->mn_ksize)
// TODO: #define NODEPGNO(node)  ((node)->mn_lo | ((pgno_t) (node)->mn_hi << 16) | (PGNO_TOPWORD ? ((pgno_t) (node)->mn_flags << PGNO_TOPWORD) : 0))
// TODO: #define SETPGNO(node,pgno)	do { (node)->mn_lo = (pgno) & 0xffff; (node)->mn_hi = (pgno) >> 16; if (PGNO_TOPWORD) (node)->mn_flags = (pgno) >> PGNO_TOPWORD; } while(0)
// TODO: #define NODEDSZ(node)	 ((node)->mn_lo | ((unsigned)(node)->mn_hi << 16))
// TODO: #define SETDSZ(node,size)	do { (node)->mn_lo = (size) & 0xffff; (node)->mn_hi = (size) >> 16;} while(0)
// TODO: #define NODEKSZ(node)	 ((node)->mn_ksize)

// TODO: #define LEAF2KEY(p, i, ks)	((char *)(p) + PAGEHDRSZ + ((i)*(ks)))

// TODO: #define MDB_GET_KEY(node, keyptr)	{ if ((keyptr) != NULL) { (keyptr)->mv_size = NODEKSZ(node); (keyptr)->mv_data = NODEKEY(node); } }
// TODO: #define MDB_GET_KEY2(node, key)	{ key.mv_size = NODEKSZ(node); key.mv_data = NODEKEY(node); }

// Compact the main page after deleting a node on a subpage.
// @param[in] mp The main page to operate on.
// @param[in] indx The index of the subpage on the main page.
func (n *node) shrink(index int) {
	/*
	MDB_node *node;
	MDB_page *sp, *xp;
	char *base;
	int nsize, delta;
	indx_t		 i, numkeys, ptr;

	node = NODEPTR(mp, indx);
	sp = (MDB_page *)NODEDATA(node);
	delta = SIZELEFT(sp);
	xp = (MDB_page *)((char *)sp + delta);

	// shift subpage upward
	if (IS_LEAF2(sp)) {
		nsize = NUMKEYS(sp) * sp->mp_pad;
		if (nsize & 1)
			return;		// do not make the node uneven-sized
		memmove(METADATA(xp), METADATA(sp), nsize);
	} else {
		int i;
		numkeys = NUMKEYS(sp);
		for (i=numkeys-1; i>=0; i--)
			xp->mp_ptrs[i] = sp->mp_ptrs[i] - delta;
	}
	xp->mp_upper = sp->mp_lower;
	xp->mp_lower = sp->mp_lower;
	xp->mp_flags = sp->mp_flags;
	xp->mp_pad = sp->mp_pad;
	COPY_PGNO(xp->mp_pgno, mp->mp_pgno);

	nsize = NODEDSZ(node) - delta;
	SETDSZ(node, nsize);

	// shift lower nodes upward
	ptr = mp->mp_ptrs[indx];
	numkeys = NUMKEYS(mp);
	for (i = 0; i < numkeys; i++) {
		if (mp->mp_ptrs[i] <= ptr)
			mp->mp_ptrs[i] += delta;
	}

	base = (char *)mp + mp->mp_upper;
	memmove(base + delta, base, ptr - mp->mp_upper + NODESIZE + NODEKSZ(node));
	mp->mp_upper += delta;
	*/
}

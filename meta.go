package bolt

type meta struct {
	magic          int32
	version        int32
	mapSize        int
	free           *bucket
	main           *bucket
	lastPageNumber int
	transactionID  int
}

// TODO: #define mm_psize mm_dbs[0].md_pad
// TODO: #define mm_flags mm_dbs[0].md_flags

// TODO:
// typedef union MDB_metabuf {
// 	MDB_page	mb_page;
// 	struct {
// 		char		mm_pad[PAGEHDRSZ];
// 		MDB_meta	mm_meta;
// 	} mb_metabuf;
// } MDB_metabuf;

// TODO:
// typedef struct MDB_dbx {
// 	MDB_val		md_name;		/**< name of the database */
// 	MDB_cmp_func	*md_cmp;	/**< function for comparing keys */
// 	MDB_cmp_func	*md_dcmp;	/**< function for comparing data items */
// 	MDB_rel_func	*md_rel;	/**< user relocate function */
// 	void		*md_relctx;		/**< user-provided context for md_rel */
// } MDB_dbx;

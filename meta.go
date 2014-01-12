package bolt

var (
	InvalidMetaPageError = &Error{"Invalid meta page", nil}
)

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

const magic uint32 = 0xC0DEC0DE
const version uint32 = 1

type meta struct {
	magic   uint32
	version uint32
	free    Bucket
	main    Bucket
	pgno    int
	txnid   int
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return InvalidError
	} else if m.version != Version {
		return VersionMismatchError
	}
	return nil
}

// Read the environment parameters of a DB environment before
// mapping it into memory.
// @param[in] env the environment handle
// @param[out] meta address of where to store the meta information
// @return 0 on success, non-zero on failure.
func (m *meta) read(p *page) error {
	/*
			if (off == 0 || m->mm_txnid > meta->mm_txnid)
				*meta = *m;
		}
		return 0;
	*/
	return nil
}

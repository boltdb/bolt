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
	db             *db
	parent         *transaction
	child          *transaction
	nextPageNumber int
	freePages      []int
	spillPages     []int
	dirtyList      []int
	reader         *reader
	// TODO: bucketxs []*bucketx
	buckets     []*bucket
	bucketFlags []int
	cursors     []*cursor
	// Implicit from slices? TODO: MDB_dbi mt_numdbs;
	mt_dirty_room int
}

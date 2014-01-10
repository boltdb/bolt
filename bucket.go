package bolt

// TODO: #define MDB_VALID	0x8000		/**< DB handle is valid, for me_dbflags */
// TODO: #define PERSISTENT_FLAGS	(0xffff & ~(MDB_VALID))
// TODO: #define VALID_FLAGS	(MDB_REVERSEKEY|MDB_DUPSORT|MDB_INTEGERKEY|MDB_DUPFIXED|MDB_INTEGERDUP|MDB_REVERSEDUP|MDB_CREATE)
// TODO: #define FREE_DBI 0
// TODO: #define MAIN_DBI 1

type Bucket struct {
	pad          uint32
	flags        uint16
	depth        uint16
	branches     pgno
	leafs        pgno
	overflows    pgno
	entries      uint64
	root         pgno
}

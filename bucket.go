package bolt

const (
	MDB_DUPSORT = 0x04
)

// TODO: #define MDB_VALID	0x8000		/**< DB handle is valid, for me_dbflags */
// TODO: #define PERSISTENT_FLAGS	(0xffff & ~(MDB_VALID))
// TODO: #define VALID_FLAGS	(MDB_REVERSEKEY|MDB_DUPSORT|MDB_INTEGERKEY|MDB_DUPFIXED|MDB_INTEGERDUP|MDB_REVERSEDUP|MDB_CREATE)
// TODO: #define FREE_DBI 0

type Bucket struct {
	*bucket
	transaction *Transaction
	name        string
	isNew       bool
	dirty       bool
	valid       bool
}

type bucket struct {
	id        uint32
	pad       uint32
	flags     uint16
	depth     uint16
	branches  pgno
	leafs     pgno
	overflows pgno
	entries   uint64
	root      pgno
}


package bolt

// TODO: #define CURSOR_STACK		 32

// TODO: #define C_INITIALIZED	0x01	/**< cursor has been initialized and is valid */
// TODO: #define C_EOF	0x02			/**< No more data */
// TODO: #define C_SUB	0x04			/**< Cursor is a sub-cursor */
// TODO: #define C_DEL	0x08			/**< last op was a cursor_del */
// TODO: #define C_SPLITTING	0x20		/**< Cursor is in page_split */
// TODO: #define C_UNTRACK	0x40		/**< Un-track cursor when closing */

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

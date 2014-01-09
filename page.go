package bolt

import (
	"unsafe"
)

const MinPageKeys = 2
const FillThreshold = 250 // 25%

const (
	BranchPage   = 0x01
	LeafPage     = 0x02
	OverflowPage = 0x04
	MetaPage     = 0x08
	DirtyPage    = 0x10 /**< dirty page, also set for #P_SUBP pages */
	SubPage      = 0x40
	KeepPage     = 0x8000 /**< leave this page alone during spill */
)

// maxCommitPages is the maximum number of pages to commit in one writev() call.
const maxCommitPages 64

/* max bytes to write in one call */
const maxWriteByteCount 0x80000000U    // TODO: #define MAX_WRITE 0x80000000U >> (sizeof(ssize_t) == 4))

// TODO:
// #if defined(IOV_MAX) && IOV_MAX < MDB_COMMIT_PAGES
// #undef MDB_COMMIT_PAGES
// #define MDB_COMMIT_PAGES	IOV_MAX
// #endif

// TODO: #define MDB_PS_MODIFY	1
// TODO: #define MDB_PS_ROOTONLY	2
// TODO: #define MDB_PS_FIRST	4
// TODO: #define MDB_PS_LAST		8

// TODO: #define MDB_SPLIT_REPLACE	MDB_APPENDDUP	/**< newkey is not new */


type page struct {
	header struct {
		id                int
		next              *page // (?)
		lower             int
		upper             int
		overflowPageCount int
	}
	metadata []byte
}

type pageState struct {
	head int  /**< Reclaimed freeDB pages, or NULL before use */
	last int  /**< ID of last used record, or 0 if !mf_pghead */
}

// nodeCount returns the number of nodes on the page.
func (p *page) nodeCount() int {
	return 0 // (p.header.lower - unsafe.Sizeof(p.header) >> 1
}

// remainingSize returns the number of bytes left in the page.
func (p *page) remainingSize() int {
	return p.header.upper - p.header.lower
}

// remainingSize returns the number of bytes left in the page.
func (p *page) remainingSize() int {
	return p.header.upper - p.header.lower
}


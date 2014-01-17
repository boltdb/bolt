package bolt

import (
	"bytes"
	"unsafe"
)

const maxPageSize = 0x8000
const minKeyCount = 2

var _page page

const pageHeaderSize = int(unsafe.Offsetof(_page.ptr))

const minPageKeys = 2
const fillThreshold = 250 // 25%

const (
	p_branch   = 0x01
	p_leaf     = 0x02
	p_overflow = 0x04
	p_meta     = 0x08
	p_dirty    = 0x10 /**< dirty page, also set for #P_SUBP pages */
	p_sub      = 0x40
	p_keep     = 0x8000 /**< leave this page alone during spill */

	p_invalid = ^pgno(0)
)

// maxCommitPages is the maximum number of pages to commit in one writev() call.
const maxCommitPages = 64

/* max bytes to write in one call */
const maxWriteByteCount uint = 0x80000000 // TODO: #define MAX_WRITE 0x80000000U >> (sizeof(ssize_t) == 4))

// TODO:
// #if defined(IOV_MAX) && IOV_MAX < MDB_COMMIT_PAGES
// #undef MDB_COMMIT_PAGES
// #define MDB_COMMIT_PAGES	IOV_MAX
// #endif

const (
	MDB_PS_MODIFY   = 1
	MDB_PS_ROOTONLY = 2
	MDB_PS_FIRST    = 4
	MDB_PS_LAST     = 8
)

// TODO: #define MDB_SPLIT_REPLACE	MDB_APPENDDUP	/**< newkey is not new */

type pgno uint64
type txnid uint64
type indx uint16

type page struct {
	id       pgno
	flags    uint16
	lower    indx
	upper    indx
	overflow uint32
	ptr      uintptr
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() (*meta, error) {
	// Exit if page is not a meta page.
	if (p.flags & p_meta) == 0 {
		return nil, InvalidMetaPageError
	}

	// Cast the meta section and validate before returning.
	m := (*meta)(unsafe.Pointer(&p.ptr))
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// init initializes a page as a new meta page.
func (p *page) init(pageSize int) {
	p.flags = p_meta
	m := (*meta)(unsafe.Pointer(&p.ptr))
	m.magic = magic
	m.version = version
	m.free.pad = uint32(pageSize)
	m.pgno = 1
	m.free.root = p_invalid
	m.buckets.root = p_invalid
}

// branchNode retrieves the branch node at the given index within the page.
func (p *page) branchNode(index indx) *branchNode {
	b := (*[maxPageSize]byte)(unsafe.Pointer(&p.ptr))
	return (*branchNode)(unsafe.Pointer(&b[index * indx(unsafe.Sizeof(index))]))
}

// leafNode retrieves the leaf node at the given index within the page.
func (p *page) leafNode(index indx) *leafNode {
	b := (*[maxPageSize]byte)(unsafe.Pointer(&p.ptr))
	return (*leafNode)(unsafe.Pointer(&b[index * indx(unsafe.Sizeof(index))]))
}

// numkeys returns the number of nodes in the page.
func (p *page) numkeys() int {
	return int((p.lower - indx(pageHeaderSize)) >> 1)
}

// remainingSize returns the number of bytes left in the page.
func (p *page) remainingSize() int {
	return int(p.upper - p.lower)
}

// find returns the node with the smallest entry larger or equal to the key.
// This function also returns a boolean stating if an exact match was made.
func (p *page) find(key []byte, pageSize int) (*node, int, bool) {
	// TODO: MDB_page *mp = mc->mc_pg[mc->mc_top];

	var node *node
	nkeys := p.numkeys()
	low, high := 1, nkeys - 1
	if (p.flags & p_leaf) != 0 {
		low = 0
	}

	// Perform a binary search to find the correct node.
	var i, rc int
	for ; low <= high; {
		i = (low + high) / 2

		node = p.node(indx(i))
		rc = bytes.Compare(key, node.key())
		if rc == 0 {
			break;
		} else if rc > 0 {
			low = i + 1
		} else {
			high = i - 1
		}
	}

	// Found entry is less than key so grab the next one.
	if rc > 0 {
		i++
	}

	// If index is beyond key range then return nil.
	if i >= nkeys {
		node = nil
	}

	exact := (rc == 0 && nkeys > 0)
	return node, i, exact
}

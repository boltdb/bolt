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

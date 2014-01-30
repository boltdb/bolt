package bolt

import (
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const maxAllocSize = 0xFFFFFFF
const minKeysPerPage = 2
const maxNodesPerPage = 65535

const (
	p_branch   = 0x01
	p_leaf     = 0x02
	p_meta     = 0x04
	p_freelist = 0x08
)

type pgid uint64

type page struct {
	id       pgid
	flags    uint16
	count    uint16
	overflow uint32
	ptr      uintptr
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// lnode retrieves the leaf node by index
func (p *page) lnode(index int) *lnode {
	return &((*[maxNodesPerPage]lnode)(unsafe.Pointer(&p.ptr)))[index]
}

// lnodes retrieves a list of leaf nodes.
func (p *page) lnodes() []lnode {
	return ((*[maxNodesPerPage]lnode)(unsafe.Pointer(&p.ptr)))[:]
}

// bnode retrieves the branch node by index
func (p *page) bnode(index int) *bnode {
	return &((*[maxNodesPerPage]bnode)(unsafe.Pointer(&p.ptr)))[index]
}

// bnodes retrieves a list of branch nodes.
func (p *page) bnodes() []bnode {
	return ((*[maxNodesPerPage]bnode)(unsafe.Pointer(&p.ptr)))[:]
}

// freelist retrieves a list of page ids from a freelist page.
func (p *page) freelist() []pgid {
	return ((*[maxNodesPerPage]pgid)(unsafe.Pointer(&p.ptr)))[0:p.count]
}

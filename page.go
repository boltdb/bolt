package bolt

import (
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const maxPageAllocSize = 0xFFFFFFF
const minKeysPerPage = 2
const maxNodesPerPage = 65535

const (
	p_branch = 0x01
	p_leaf   = 0x02
	p_meta   = 0x04
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
	m.pageSize = uint32(pageSize)
	m.pgid = 1
	m.sys.root = 0
}

// lnode retrieves the leaf node by index
func (p *page) lnode(index int) *lnode {
	return &((*[maxNodesPerPage]lnode)(unsafe.Pointer(&p.ptr)))[index]
}

package bolt

import (
	"fmt"
	"os"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const maxAllocSize = 0xFFFFFFF
const minKeysPerPage = 2
const maxNodesPerPage = 65535

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const (
	p_branch   = 0x01
	p_leaf     = 0x02
	p_meta     = 0x04
	p_buckets  = 0x08
	p_freelist = 0x10
)

type pgid uint64

type page struct {
	id       pgid
	flags    uint16
	count    uint16
	overflow uint32
	ptr      uintptr
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & p_branch) != 0 {
		return "branch"
	} else if (p.flags & p_leaf) != 0 {
		return "leaf"
	} else if (p.flags & p_meta) != 0 {
		return "meta"
	} else if (p.flags & p_buckets) != 0 {
		return "buckets"
	} else if (p.flags & p_freelist) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[maxNodesPerPage]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	return ((*[maxNodesPerPage]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[maxNodesPerPage]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	return ((*[maxNodesPerPage]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// freelist retrieves a list of page ids from a freelist page.
func (p *page) freelist() []pgid {
	return ((*[maxNodesPerPage]pgid)(unsafe.Pointer(&p.ptr)))[0:p.count]
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos+n.ksize : n.pos+n.ksize+n.vsize]
}

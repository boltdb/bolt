package bolt

import (
	"unsafe"
)

const lnodeSize = int(unsafe.Sizeof(lnode{}))

// lnode represents a node on a leaf page.
type lnode struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *lnode) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos:n.pos+n.ksize]
}

// value returns a byte slice of the node value.
func (n *lnode) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos+n.ksize:n.pos+n.ksize+n.vsize]
}

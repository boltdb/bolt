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
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n))[n.pos : n.pos+n.ksize]
}

// value returns a byte slice of the node value.
func (n *lnode) value() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n))[n.pos+n.ksize : n.pos+n.ksize+n.vsize]
}

package bolt

import (
	"unsafe"
)

const bnodeSize = int(unsafe.Sizeof(lnode{}))

// bnode represents a node on a branch page.
type bnode struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// key returns a byte slice of the node key.
func (n *bnode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n))[n.pos : n.pos+n.ksize]
}

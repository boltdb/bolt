package bolt

import (
	"unsafe"
)

// bnode represents a node on a branch page.
type bnode struct {
	flags   uint16
	keySize uint16
	pgid    pgid
	data    uintptr // Pointer to the beginning of the data.
}

// key returns a byte slice that of the key data.
func (n *bnode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[:n.keySize]
}

// bnodeSize returns the number of bytes required to store a key as a branch node.
func bnodeSize(key []byte) int {
	return int(unsafe.Offsetof((*bnode)(nil)).data) + len(key)
}

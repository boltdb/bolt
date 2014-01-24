package bolt

import (
	"unsafe"
)

type nodeid uint16

// lnode represents a node on a leaf page.
type lnode struct {
	flags    uint16
	keySize  uint16
	dataSize uint32
	data     uintptr // Pointer to the beginning of the data.
}

// key returns a byte slice that of the node key.
func (n *lnode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[:n.keySize]
}

// data returns a byte slice that of the node data.
func (n *lnode) data() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[n.keySize : n.keySize+n.dataSize]
}

// lnodeSize returns the number of bytes required to store a key+data as a leaf node.
func lnodeSize(key []byte, data []byte) int {
	return int(unsafe.Offsetof((*lnode)(nil)).data) + len(key) + len(data)
}

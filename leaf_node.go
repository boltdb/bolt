package bolt

import (
	"unsafe"
)

// leafNode represents a node on a leaf page.
type leafNode struct {
	flags    uint16
	keySize  uint16
	dataSize uint32
	data     uintptr // Pointer to the beginning of the data.
}

// key returns a byte slice that of the key data.
func (n *leafNode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[:n.keySize]
}

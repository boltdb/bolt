package bolt

import (
	"unsafe"
)

// node represents a node on a page.
type node struct {
	flags   uint16
	keySize uint16
}

// leafNode represents a node on a leaf page.
type leafNode struct {
	node
	dataSize uint32
	data     uintptr // Pointer to the beginning of the data.
}

// branchNode represents a node on a branch page.
type branchNode struct {
	node
	pgno uint32
	data uintptr // Pointer to the beginning of the data.
}

// key returns a byte slice that of the key data.
func (n *leafNode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[:n.keySize]
}

func leafNodeSize(key []byte, data []byte) int {
	// TODO: Return even(sizeof(node) + len(key) + len(data))
	return 0
}

func branchNodeSize(key []byte) int {
	// TODO: Return even(sizeof(node) + len(key))
	return 0
}

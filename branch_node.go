package bolt

import (
	"unsafe"
)

const (
	bigNode = 0x01
	subNode = 0x02
	dupNode = 0x04
)

// key returns a byte slice that of the key data.
func (n *branchNode) key() []byte {
	return (*[MaxKeySize]byte)(unsafe.Pointer(&n.data))[:n.keySize]
}

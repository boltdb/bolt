package bolt

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a temporary page can insert a key/value.
func TestTpagePut(t *testing.T) {
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("baz"), []byte("2"))
	p.put([]byte("foo"), []byte("0"))
	p.put([]byte("bar"), []byte("1"))
	p.put([]byte("foo"), []byte("3"))
	assert.Equal(t, len(p.nodes), 3)
	assert.Equal(t, p.nodes[0], tnode{[]byte("bar"), []byte("1")})
	assert.Equal(t, p.nodes[1], tnode{[]byte("baz"), []byte("2")})
	assert.Equal(t, p.nodes[2], tnode{[]byte("foo"), []byte("3")})
}

// Ensure that a temporary page can deserialize from a page.
func TestTpageRead(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.count = 2

	// Insert 2 leaf nodes at the beginning. sizeof(lnode) == 16
	nodes := (*[3]lnode)(unsafe.Pointer(&page.ptr))
	nodes[0] = lnode{flags: 0, pos: 32, ksize: 3, vsize: 4} // pos = sizeof(lnode) * 2
	nodes[1] = lnode{flags: 0, pos: 23, ksize: 10, vsize: 3} // pos = sizeof(lnode) + 3 + 4

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("barfooz"))
	copy(data[7:], []byte("helloworldbye"))

	// Deserialize page into a temporary page.
	p := &tpage{}
	p.read(page)

	// Check that there are two nodes with correct data.
	assert.Equal(t, len(p.nodes), 2)
	assert.Equal(t, p.nodes[0].key, []byte("bar"))
	assert.Equal(t, p.nodes[0].value, []byte("fooz"))
	assert.Equal(t, p.nodes[1].key, []byte("helloworld"))
	assert.Equal(t, p.nodes[1].value, []byte("bye"))
}

// Ensure that a temporary page can serialize itself.
func TestTpageWrite(t *testing.T) {
	t.Skip("pending")
}

// Ensure that a temporary page can split into appropriate subgroups.
func TestTpageSplit(t *testing.T) {
	t.Skip("pending")
}

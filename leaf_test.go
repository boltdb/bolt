package bolt

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a temporary page can insert a key/value.
func TestLeafPut(t *testing.T) {
	l := &leaf{items: make(leafItems, 0)}
	l.put([]byte("baz"), []byte("2"))
	l.put([]byte("foo"), []byte("0"))
	l.put([]byte("bar"), []byte("1"))
	l.put([]byte("foo"), []byte("3"))
	assert.Equal(t, len(l.items), 3)
	assert.Equal(t, l.items[0], leafItem{[]byte("bar"), []byte("1")})
	assert.Equal(t, l.items[1], leafItem{[]byte("baz"), []byte("2")})
	assert.Equal(t, l.items[2], leafItem{[]byte("foo"), []byte("3")})
}

// Ensure that a temporary page can deserialize from a page.
func TestLeafRead(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.count = 2

	// Insert 2 leaf items at the beginning. sizeof(lnode) == 16
	nodes := (*[3]lnode)(unsafe.Pointer(&page.ptr))
	nodes[0] = lnode{flags: 0, pos: 32, ksize: 3, vsize: 4}  // pos = sizeof(lnode) * 2
	nodes[1] = lnode{flags: 0, pos: 23, ksize: 10, vsize: 3} // pos = sizeof(lnode) + 3 + 4

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("barfooz"))
	copy(data[7:], []byte("helloworldbye"))

	// Deserialize page into a temporary page.
	l := &leaf{}
	l.read(page)

	// Check that there are two items with correct data.
	assert.Equal(t, len(l.items), 2)
	assert.Equal(t, l.items[0].key, []byte("bar"))
	assert.Equal(t, l.items[0].value, []byte("fooz"))
	assert.Equal(t, l.items[1].key, []byte("helloworld"))
	assert.Equal(t, l.items[1].value, []byte("bye"))
}

// Ensure that a temporary page can serialize itself.
func TestLeafWrite(t *testing.T) {
	// Create a temp page.
	l := &leaf{items: make(leafItems, 0)}
	l.put([]byte("susy"), []byte("que"))
	l.put([]byte("ricki"), []byte("lake"))
	l.put([]byte("john"), []byte("johnson"))

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	l.write(p)

	// Read the page back in.
	l2 := &leaf{}
	l2.read(p)

	// Check that the two pages are the same.
	assert.Equal(t, len(l2.items), 3)
	assert.Equal(t, l2.items[0].key, []byte("john"))
	assert.Equal(t, l2.items[0].value, []byte("johnson"))
	assert.Equal(t, l2.items[1].key, []byte("ricki"))
	assert.Equal(t, l2.items[1].value, []byte("lake"))
	assert.Equal(t, l2.items[2].key, []byte("susy"))
	assert.Equal(t, l2.items[2].value, []byte("que"))
}

// Ensure that a temporary page can split into appropriate subgroups.
func TestLeafSplit(t *testing.T) {
	// Create a temp page.
	l := &leaf{items: make(leafItems, 0)}
	l.put([]byte("00000001"), []byte("0123456701234567"))
	l.put([]byte("00000002"), []byte("0123456701234567"))
	l.put([]byte("00000003"), []byte("0123456701234567"))
	l.put([]byte("00000004"), []byte("0123456701234567"))
	l.put([]byte("00000005"), []byte("0123456701234567"))

	// Split between 3 & 4.
	leafs := l.split(100)

	assert.Equal(t, len(leafs), 2)
	assert.Equal(t, len(leafs[0].items), 2)
	assert.Equal(t, len(leafs[1].items), 3)
}

// Ensure that a temporary page with the minimum number of items just returns a single split group.
func TestLeafSplitWithMinKeys(t *testing.T) {
	// Create a temp page.
	l := &leaf{items: make(leafItems, 0)}
	l.put([]byte("00000001"), []byte("0123456701234567"))
	l.put([]byte("00000002"), []byte("0123456701234567"))

	// Split.
	leafs := l.split(20)
	assert.Equal(t, len(leafs), 1)
	assert.Equal(t, len(leafs[0].items), 2)
}

// Ensure that a temporary page that has keys that all fit on a page just returns one split group.
func TestLeafSplitFitsInPage(t *testing.T) {
	// Create a temp page.
	l := &leaf{items: make(leafItems, 0)}
	l.put([]byte("00000001"), []byte("0123456701234567"))
	l.put([]byte("00000002"), []byte("0123456701234567"))
	l.put([]byte("00000003"), []byte("0123456701234567"))
	l.put([]byte("00000004"), []byte("0123456701234567"))
	l.put([]byte("00000005"), []byte("0123456701234567"))

	// Split.
	leafs := l.split(4096)
	assert.Equal(t, len(leafs), 1)
	assert.Equal(t, len(leafs[0].items), 5)
}

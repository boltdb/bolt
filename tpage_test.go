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
	nodes[0] = lnode{flags: 0, pos: 32, ksize: 3, vsize: 4}  // pos = sizeof(lnode) * 2
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
	// Create a temp page.
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("susy"), []byte("que"))
	p.put([]byte("ricki"), []byte("lake"))
	p.put([]byte("john"), []byte("johnson"))

	// Write it to a page.
	var buf [4096]byte
	allocate := func(size int) (*page, error) {
		return (*page)(unsafe.Pointer(&buf[0])), nil
	}
	pages, err := p.write(4096, allocate)
	assert.NoError(t, err)

	// Read the page back in.
	p2 := &tpage{}
	p2.read(pages[0])

	// Check that the two pages are the same.
	assert.Equal(t, len(p2.nodes), 3)
	assert.Equal(t, p2.nodes[0].key, []byte("john"))
	assert.Equal(t, p2.nodes[0].value, []byte("johnson"))
	assert.Equal(t, p2.nodes[1].key, []byte("ricki"))
	assert.Equal(t, p2.nodes[1].value, []byte("lake"))
	assert.Equal(t, p2.nodes[2].key, []byte("susy"))
	assert.Equal(t, p2.nodes[2].value, []byte("que"))
}

// Ensure that an error that an allocation error during writing is returned.
func TestTpageWriteError(t *testing.T) {
	// Create a temp page.
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("susy"), []byte("que"))

	// Write it to a page.
	exp := &Error{}
	allocate := func(size int) (*page, error) {
		return nil, exp
	}
	pages, err := p.write(4096, allocate)
	assert.Nil(t, pages)
	assert.Equal(t, err, exp)
}

// Ensure that a temporary page can split into appropriate subgroups.
func TestTpageSplit(t *testing.T) {
	// Create a temp page.
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("00000001"), []byte("0123456701234567"))
	p.put([]byte("00000002"), []byte("0123456701234567"))
	p.put([]byte("00000003"), []byte("0123456701234567"))
	p.put([]byte("00000004"), []byte("0123456701234567"))
	p.put([]byte("00000005"), []byte("0123456701234567"))

	// Split between 3 & 4.
	pages := p.split(100)

	assert.Equal(t, len(pages), 2)
	assert.Equal(t, len(pages[0]), 2)
	assert.Equal(t, len(pages[1]), 3)
}

// Ensure that a temporary page with the minimum number of nodes just returns a single split group.
func TestTpageSplitWithMinKeys(t *testing.T) {
	// Create a temp page.
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("00000001"), []byte("0123456701234567"))
	p.put([]byte("00000002"), []byte("0123456701234567"))

	// Split.
	pages := p.split(20)
	assert.Equal(t, len(pages), 1)
	assert.Equal(t, len(pages[0]), 2)
}

// Ensure that a temporary page that has keys that all fit on a page just returns one split group.
func TestTpageSplitFitsInPage(t *testing.T) {
	// Create a temp page.
	p := &tpage{nodes: make(tnodes, 0)}
	p.put([]byte("00000001"), []byte("0123456701234567"))
	p.put([]byte("00000002"), []byte("0123456701234567"))
	p.put([]byte("00000003"), []byte("0123456701234567"))
	p.put([]byte("00000004"), []byte("0123456701234567"))
	p.put([]byte("00000005"), []byte("0123456701234567"))

	// Split.
	pages := p.split(4096)
	assert.Equal(t, len(pages), 1)
	assert.Equal(t, len(pages[0]), 5)
}

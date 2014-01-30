package bolt

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a branch can replace a key.
func TestBranchPutReplace(t *testing.T) {
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("bar")},
			branchItem{pgid: 2, key: []byte("baz")},
			branchItem{pgid: 3, key: []byte("foo")},
		},
	}
	b.put(1, 4, []byte("bar"), true)
	b.put(2, 5, []byte("boo"), true)
	assert.Equal(t, len(b.items), 3)
	assert.Equal(t, b.items[0].pgid, pgid(4))
	assert.Equal(t, string(b.items[0].key), "bar")
	assert.Equal(t, b.items[1].pgid, pgid(5))
	assert.Equal(t, string(b.items[1].key), "boo")
	assert.Equal(t, b.items[2].pgid, pgid(3))
	assert.Equal(t, string(b.items[2].key), "foo")
}

// Ensure that a branch can insert a key.
func TestBranchPutInsert(t *testing.T) {
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("bar")},
			branchItem{pgid: 2, key: []byte("foo")},
		},
	}
	b.put(1, 4, []byte("baz"), false)
	b.put(2, 5, []byte("zzz"), false)
	assert.Equal(t, len(b.items), 4)
	assert.Equal(t, b.items[0].pgid, pgid(1))
	assert.Equal(t, string(b.items[0].key), "bar")
	assert.Equal(t, b.items[1].pgid, pgid(4))
	assert.Equal(t, string(b.items[1].key), "baz")
	assert.Equal(t, b.items[2].pgid, pgid(2))
	assert.Equal(t, string(b.items[2].key), "foo")
	assert.Equal(t, b.items[3].pgid, pgid(5))
	assert.Equal(t, string(b.items[3].key), "zzz")
}

// Ensure that a branch can deserialize from a page.
func TestBranchRead(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.count = 2

	// Insert 2 items at the beginning. sizeof(bnode) == 16
	nodes := (*[3]bnode)(unsafe.Pointer(&page.ptr))
	nodes[0] = bnode{pos: 32, ksize: 3, pgid: 100}  // pos = sizeof(bnode) * 2
	nodes[1] = bnode{pos: 19, ksize: 10, pgid: 101} // pos = sizeof(bnode) + 3

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("bar"))
	copy(data[3:], []byte("helloworld"))

	// Deserialize page into a branch.
	b := &branch{}
	b.read(page)

	// Check that there are two items with correct data.
	assert.Equal(t, len(b.items), 2)
	assert.Equal(t, b.items[0].key, []byte("bar"))
	assert.Equal(t, b.items[1].key, []byte("helloworld"))
}

// Ensure that a branch can serialize itself.
func TestBranchWrite(t *testing.T) {
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("susy")},
			branchItem{pgid: 2, key: []byte("ricki")},
			branchItem{pgid: 3, key: []byte("john")},
		},
	}

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	b.write(p)

	// Read the page back in.
	b2 := &branch{}
	b2.read(p)

	// Check that the two pages are the same.
	assert.Equal(t, len(b2.items), 3)
	assert.Equal(t, b2.items[0].pgid, pgid(1))
	assert.Equal(t, b2.items[0].key, []byte("susy"))
	assert.Equal(t, b2.items[1].pgid, pgid(2))
	assert.Equal(t, b2.items[1].key, []byte("ricki"))
	assert.Equal(t, b2.items[2].pgid, pgid(3))
	assert.Equal(t, b2.items[2].key, []byte("john"))
}

// Ensure that a branch can split into appropriate subgroups.
func TestBranchSplit(t *testing.T) {
	// Create a branch.
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("00000001")},
			branchItem{pgid: 2, key: []byte("00000002")},
			branchItem{pgid: 3, key: []byte("00000003")},
			branchItem{pgid: 4, key: []byte("00000004")},
			branchItem{pgid: 5, key: []byte("00000005")},
		},
	}

	// Split between 3 & 4.
	branches := b.split(100)

	assert.Equal(t, len(branches), 2)
	assert.Equal(t, len(branches[0].items), 2)
	assert.Equal(t, len(branches[1].items), 3)
}

// Ensure that a branch with the minimum number of items just returns a single branch.
func TestBranchSplitWithMinKeys(t *testing.T) {
	// Create a branch.
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("00000001")},
			branchItem{pgid: 2, key: []byte("00000002")},
		},
	}

	// Split.
	branches := b.split(20)
	assert.Equal(t, len(branches), 1)
	assert.Equal(t, len(branches[0].items), 2)
}

// Ensure that a branch that has keys that all fit on a page just returns one branch.
func TestBranchSplitFitsInPage(t *testing.T) {
	// Create a branch.
	b := &branch{
		items: branchItems{
			branchItem{pgid: 1, key: []byte("00000001")},
			branchItem{pgid: 2, key: []byte("00000002")},
			branchItem{pgid: 3, key: []byte("00000003")},
			branchItem{pgid: 4, key: []byte("00000004")},
			branchItem{pgid: 5, key: []byte("00000005")},
		},
	}

	// Split.
	branches := b.split(4096)
	assert.Equal(t, len(branches), 1)
	assert.Equal(t, len(branches[0].items), 5)
}

package bolt

import (
	"testing"

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
	t.Skip("pending")
}

// Ensure that a temporary page can serialize itself.
func TestTpageWrite(t *testing.T) {
	t.Skip("pending")
}

// Ensure that a temporary page can split into appropriate subgroups.
func TestTpageSplit(t *testing.T) {
	t.Skip("pending")
}

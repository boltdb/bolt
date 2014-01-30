package bolt

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a system page can set a bucket.
func TestSysPut(t *testing.T) {
	s := &sys{buckets: make(map[string]*bucket)}
	s.put("foo", &bucket{root: 2})
	s.put("bar", &bucket{root: 3})
	s.put("foo", &bucket{root: 4})
	assert.Equal(t, len(s.buckets), 2)
	assert.Equal(t, s.get("foo").root, pgid(4))
	assert.Equal(t, s.get("bar").root, pgid(3))
	assert.Nil(t, s.get("no_such_bucket"))
}

// Ensure that a system page can deserialize from a page.
func TestSysRead(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.count = 2

	// Insert 2 buckets at the beginning.
	buckets := (*[3]bucket)(unsafe.Pointer(&page.ptr))
	buckets[0] = bucket{root: 3}
	buckets[1] = bucket{root: 4}

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&buckets[2]))
	data[0] = 3
	copy(data[1:], []byte("bar"))
	data[4] = 10
	copy(data[5:], []byte("helloworld"))

	// Deserialize page into a system page.
	s := &sys{buckets: make(map[string]*bucket)}
	s.read(page)

	// Check that there are two items with correct data.
	assert.Equal(t, len(s.buckets), 2)
	assert.Equal(t, s.get("bar").root, pgid(3))
	assert.Equal(t, s.get("helloworld").root, pgid(4))
}

// Ensure that a system page can serialize itself.
func TestSysWrite(t *testing.T) {
	s := &sys{buckets: make(map[string]*bucket)}
	s.put("foo", &bucket{root: 2})
	s.put("bar", &bucket{root: 3})

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	s.write(p)

	// Read the page back in.
	s2 := &sys{buckets: make(map[string]*bucket)}
	s2.read(p)

	// Check that the two pages are the same.
	assert.Equal(t, len(s.buckets), 2)
	assert.Equal(t, s.get("foo").root, pgid(2))
	assert.Equal(t, s.get("bar").root, pgid(3))
}

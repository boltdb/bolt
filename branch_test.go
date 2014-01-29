package bolt

import (
	"testing"

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

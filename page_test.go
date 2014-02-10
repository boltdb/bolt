package bolt

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Ensure that the page type can be returned in human readable format.
func TestPageTyp(t *testing.T) {
	assert.Equal(t, (&page{flags: p_branch}).typ(), "branch")
	assert.Equal(t, (&page{flags: p_leaf}).typ(), "leaf")
	assert.Equal(t, (&page{flags: p_meta}).typ(), "meta")
	assert.Equal(t, (&page{flags: p_buckets}).typ(), "buckets")
	assert.Equal(t, (&page{flags: p_freelist}).typ(), "freelist")
	assert.Equal(t, (&page{flags: 20000}).typ(), "unknown<4e20>")
}

// Ensure that the hexdump debugging function doesn't blow up.
func TestPageDump(t *testing.T) {
	(&page{id: 256}).hexdump(16)
}

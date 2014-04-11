package bolt

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Ensure that the page type can be returned in human readable format.
func TestPage_typ(t *testing.T) {
	assert.Equal(t, (&page{flags: branchPageFlag}).typ(), "branch")
	assert.Equal(t, (&page{flags: leafPageFlag}).typ(), "leaf")
	assert.Equal(t, (&page{flags: metaPageFlag}).typ(), "meta")
	assert.Equal(t, (&page{flags: freelistPageFlag}).typ(), "freelist")
	assert.Equal(t, (&page{flags: 20000}).typ(), "unknown<4e20>")
}

// Ensure that the hexdump debugging function doesn't blow up.
func TestPage_dump(t *testing.T) {
	(&page{id: 256}).hexdump(16)
}

package bolt

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Ensure that meta with bad magic is invalid.
func TestMeta_validate_magic(t *testing.T) {
	m := &meta{magic: 0x01234567}
	assert.Equal(t, m.validate(), ErrInvalid)
}

// Ensure that meta with a bad version is invalid.
func TestMeta_validate_version(t *testing.T) {
	m := &meta{magic: magic, version: 200}
	assert.Equal(t, m.validate(), ErrVersionMismatch)
}

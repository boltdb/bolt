package bolt

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Ensure that nested errors are appropriately formatted.
func TestError(t *testing.T) {
	e := &Error{"one error", &Error{"two error", nil}}
	assert.Equal(t, e.Error(), "one error: two error")
}

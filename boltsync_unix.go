// +build !windows,!plan9,!linux

package bolt

import (
	"os"
)

var odirect int

// fdatasync flushes written data to a file descriptor.
func fdatasync(f *os.File) error {
	return f.Sync()
}

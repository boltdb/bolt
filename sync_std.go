// +build !linux

package bolt

import "os"

// Fall back to syncing metadata too.
func fdatasync(f *os.File) error {
	return f.Sync()
}

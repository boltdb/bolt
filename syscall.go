// +build !linux

package bolt

import "os"

func fdatasync(f *os.File) error {
	return f.Sync()
}

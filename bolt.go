// +build !linux

package bolt

import (
	"os"
)

var odirect int

func fdatasync(f *os.File) error {
	return f.Sync()
}

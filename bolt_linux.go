package bolt

import (
	"os"
	"syscall"
)

var odirect = syscall.O_DIRECT

// fdatasync flushes written data to a file descriptor.
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

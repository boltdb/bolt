package bolt

import (
	"os"
	"syscall"
)

var odirect = syscall.O_DIRECT

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

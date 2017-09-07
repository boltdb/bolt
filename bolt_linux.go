package bolt

import (
    "syscall"
)

// fsync flushes written data to a file descriptor.
func fsync(file *tDbFile) error {
    return syscall.Fdatasync(int(file.file.Fd()))
}

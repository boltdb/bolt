package bolt

import (
	"os"
	"syscall"
)

var odirect int

// fdatasync flushes written data to a file descriptor.
func fdatasync(f *os.File) error {
	return f.Sync()
}

// flock acquires an advisory lock on a file descriptor.
func flock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// funlock releases an advisory lock on a file descriptor.
func funlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a file to a byte slice.
func mmap(f *os.File, sz int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
}

// munmap unmaps a pointer from a file.
func munmap(b []byte) error {
	return syscall.Munmap(b)
}

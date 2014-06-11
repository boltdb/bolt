package bolt

import (
	"os"
	"syscall"
	"unsafe"
)

var odirect int

// fdatasync flushes written data to a file descriptor.
func fdatasync(f *os.File) error {
	return f.Sync()
}

// flock acquires an advisory lock on a file descriptor.
func flock(f *os.File) error {
	return nil
}

// funlock releases an advisory lock on a file descriptor.
func funlock(f *os.File) error {
	return nil
}

// mmap memory maps a file to a byte slice.
// Based on: https://github.com/edsrzf/mmap-go
func mmap(f *os.File, sz int) ([]byte, error) {
	// Open a file mapping handle.
	sizelo, sizehi := uint32(sz>>32), uint32(sz&0xffffffff)
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, sizehi, sizelo, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(sz))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte slice.
	b := ((*[0xFFFFFFF]byte)(unsafe.Pointer(addr)))[0:sz]
	return b, nil
}

// munmap unmaps a pointer from a file.
// Based on: https://github.com/edsrzf/mmap-go
func munmap(b []byte) error {
	addr := (uintptr)(unsafe.Pointer(&b[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}

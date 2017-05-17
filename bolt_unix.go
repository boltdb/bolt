// +build !windows,!plan9,!solaris

package bolt

import (
    "fmt"
    "os"
    "reflect"
    "syscall"
    "time"
    "unsafe"
)

// flock acquires an advisory lock on a file descriptor.
func flock(file *tDbFile, mode os.FileMode, exclusive bool, timeout time.Duration) error {
    var t time.Time
    for {
        // If we're beyond our timeout then return an error.
        // This can only occur after we've attempted a flock once.
        if t.IsZero() {
            t = time.Now()
        } else if timeout > 0 && time.Since(t) > timeout {
            return ErrTimeout
        }
        flag := syscall.LOCK_SH
        if exclusive {
            flag = syscall.LOCK_EX
        }

        // Otherwise attempt to obtain an exclusive lock.
        err := syscall.Flock(int(file.file.Fd()), flag|syscall.LOCK_NB)
        if err == nil {
            return nil
        } else if err != syscall.EWOULDBLOCK {
            return err
        }

        // Wait for a bit and try again.
        time.Sleep(50 * time.Millisecond)
    }
}

// funlock releases an advisory lock on a file descriptor.
func funlock(file *tDbFile) error {
    return syscall.Flock(int(file.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DbFile's data file.
func mmap(file *tDbFile, sz, flags int) (unsafe.Pointer, error) {
    // Map the data file to memory.
    b, err := syscall.Mmap(int(file.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|flags)
    if err != nil {
        return nil, err
    }

    // Advise the kernel that the mmap is accessed randomly.
    if err := madvise(b, syscall.MADV_RANDOM); err != nil {
        return nil, fmt.Errorf("madvise: %s", err)
    }

    // Return the byte array pointer.
    return unsafe.Pointer(&b[0]), nil
}

// munmap unmaps a DbFile's data file from memory.
func munmap(ptr unsafe.Pointer, size int) error {
    // Original slice descriptor
    slice_desc := reflect.SliceHeader{
        Data: uintptr(ptr),
        Len:  size,
        Cap:  size,
    }

    // Reconstruct original byte slice
    original_slice := *(*[]byte)(unsafe.Pointer(&slice_desc))

    // Unmap using the original byte slice.
    return syscall.Munmap(original_slice)
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
    _, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
    if e1 != 0 {
        err = e1
    }
    return
}

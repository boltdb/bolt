package bolt

import (
    "fmt"
    "os"
    "syscall"
    "time"
    "unsafe"
)

// LockFileEx code derived from golang build filemutex_windows.go @ v1.5.1
var (
    modkernel32      = syscall.NewLazyDLL("kernel32.dll")
    procLockFileEx   = modkernel32.NewProc("LockFileEx")
    procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
    lockExt = ".lock"

    // see https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
    flagLockExclusive       = 2
    flagLockFailImmediately = 1

    // see https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382(v=vs.85).aspx
    errLockViolation syscall.Errno = 0x21
)

func lockFileEx(h syscall.Handle, flags, reserved, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
    r, _, err := procLockFileEx.Call(uintptr(h), uintptr(flags), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
    if r == 0 {
        return err
    }
    return nil
}

func unlockFileEx(h syscall.Handle, reserved, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
    r, _, err := procUnlockFileEx.Call(uintptr(h), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)), 0)
    if r == 0 {
        return err
    }
    return nil
}

// fsync flushes written data to a file descriptor.
func fsync(file *tDbFile) error {
    return file.file.Sync()
}

// flock acquires an advisory lock on a file descriptor.
func flock(file *tDbFile, mode os.FileMode, exclusive bool, timeout time.Duration) error {
    // Create a separate lock file on windows because a process
    // cannot share an exclusive lock on the same file. This is
    // needed during Tx.WriteTo().
    f, err := os.OpenFile(file.Name()+lockExt, os.O_CREATE, mode)
    if err != nil {
        return err
    }
    file.lockfile = f

    var t time.Time
    for {
        // If we're beyond our timeout then return an error.
        // This can only occur after we've attempted a flock once.
        if t.IsZero() {
            t = time.Now()
        } else if timeout > 0 && time.Since(t) > timeout {
            return ErrTimeout
        }

        var flag uint32 = flagLockFailImmediately
        if exclusive {
            flag |= flagLockExclusive
        }

        err := lockFileEx(syscall.Handle(file.lockfile.Fd()), flag, 0, 1, 0, &syscall.Overlapped{})
        if err == nil {
            return nil
        } else if err != errLockViolation {
            return err
        }

        // Wait for a bit and try again.
        time.Sleep(50 * time.Millisecond)
    }
}

// funlock releases an advisory lock on a file descriptor.
func funlock(file *tDbFile) error {
    err := unlockFileEx(syscall.Handle(file.lockfile.Fd()), 0, 1, 0, &syscall.Overlapped{})
    file.lockfile.Close()
    os.Remove(file.Name() + lockExt)
    return err
}

// mmap memory maps a DbFile's data file.
// Based on: https://github.com/edsrzf/mmap-go
func mmap(file *tDbFile, sz, flags int) (unsafe.Pointer, error) {
    if file.access_mode != MODE_RDONLY {
        // Truncate the database to the size of the mmap.
        if err := file.file.Truncate(int64(sz)); err != nil {
            return nullPtr, fmt.Errorf("truncate: %s", err)
        }
    }

    // Open a file mapping handle.
    sizelo := uint32(sz >> 32)
    sizehi := uint32(sz) & 0xffffffff
    prot := uint32(syscall.PAGE_READONLY)
    h, errno := syscall.CreateFileMapping(syscall.Handle(file.file.Fd()), nil, prot, sizelo, sizehi, nil)
    if h == 0 {
        return nullPtr, os.NewSyscallError("CreateFileMapping", errno)
    }

    // Create the memory map.
    addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(sz))
    if addr == 0 {
        return nullPtr, os.NewSyscallError("MapViewOfFile", errno)
    }

    // Close mapping handle.
    if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
        return nullPtr, os.NewSyscallError("CloseHandle", err)
    }

    return unsafe.Pointer(addr), nil
}

// munmap unmaps a pointer.
// Based on: https://github.com/edsrzf/mmap-go
func munmap(ptr unsafe.Pointer, size int) error {
    addr := uintptr(ptr)
    if err := syscall.UnmapViewOfFile(addr); err != nil {
        return os.NewSyscallError("UnmapViewOfFile", err)
    }
    return nil
}

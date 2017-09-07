package bolt

import (
    "io"
    "os"
    "time"
    "unsafe"
)

var (
    // Converting AccessMode to open file flag
    accessmode_to_openflag = map[AccessMode]int{
        MODE_RDONLY: os.O_RDONLY,
        MODE_WRONLY: os.O_WRONLY,
        MODE_RDWR:   os.O_RDWR,
    }
)

// Access mode type to define how access to a file (read/write/both)
type AccessMode int

const (
    // No access
    MODE_NONE AccessMode = iota

    // Read-only access
    MODE_RDONLY

    // Write-only access
    MODE_WRONLY

    // Full access (Read/Write)
    MODE_RDWR
)

func (mode AccessMode) to_openflag() int {
    res, ok := accessmode_to_openflag[mode]
    if !ok {
        return 0
    }

    return res
}

// Abstraction layer for using a file
// This helps to use virtual or not physical file
// For example, you can plug Bolt with abstract filesystems like:
// Afero (https://github.com/spf13/afero),
// GoBilly (https://github.com/src-d/go-billy).
type DbFile interface {
    io.Closer
    io.Reader
    io.ReaderAt
    io.Seeker
    io.Writer
    io.WriterAt

    Mode() AccessMode

    Name() string
    Stat() (os.FileInfo, error)
    Sync() error
    Truncate(size int64) error

    Lock(exclusive bool, timeout time.Duration) error
    Unlock() error

    Map(size, flags int) (unsafe.Pointer, error)
    UnMap(ptr unsafe.Pointer, size int) error

    Open(mode AccessMode, flag int) (DbFile, error)
}

// Concrete implementation of DbFile for standard use
type tDbFile struct {
    file        *os.File
    lockfile    *os.File // windows only
    access_mode AccessMode
    perm        os.FileMode
}

func (self *tDbFile) Close() error                                   { return self.file.Close() }
func (self *tDbFile) Read(p []byte) (n int, err error)               { return self.file.Read(p) }
func (self *tDbFile) Write(p []byte) (n int, err error)              { return self.file.Write(p) }
func (self *tDbFile) ReadAt(p []byte, off int64) (n int, err error)  { return self.ReadAt(p, off) }
func (self *tDbFile) WriteAt(p []byte, off int64) (n int, err error) { return self.WriteAt(p, off) }
func (self *tDbFile) Seek(offset int64, whence int) (int64, error)   { return self.Seek(offset, whence) }
func (self *tDbFile) Mode() AccessMode                               { return self.access_mode }
func (self *tDbFile) Name() string                                   { return self.file.Name() }
func (self *tDbFile) Stat() (os.FileInfo, error)                     { return self.file.Stat() }
func (self *tDbFile) Sync() error                                    { return fsync(self) }
func (self *tDbFile) Truncate(size int64) error                      { return self.file.Truncate(size) }
func (self *tDbFile) Lock(excl bool, to time.Duration) error         { return flock(self, self.perm, excl, to) }
func (self *tDbFile) Unlock() error                                  { return funlock(self) }
func (self *tDbFile) Map(size, flags int) (unsafe.Pointer, error)    { return mmap(self, size, flags) }
func (self *tDbFile) UnMap(addr unsafe.Pointer, size int) error      { return munmap(addr, size) }

func (self *tDbFile) Open(mode AccessMode, flag int) (DbFile, error) {
    // Open the file with the same function which opens this file
    return OpenDbFile(self.file.Name(), mode.to_openflag()|flag, 0)
}

// Open a standard DbFile from the current OS filesystem
func OpenDbFile(path string, flag int, perm os.FileMode) (DbFile, error) {
    // Determine the access mode from opening flags
    access_mode := MODE_RDONLY
    switch {
    case (flag & os.O_RDONLY) != 0:
        access_mode = MODE_RDONLY

    case (flag & os.O_WRONLY) != 0:
        access_mode = MODE_WRONLY
    }

    // Open the file in standard way
    file, err := os.OpenFile(path, flag|os.O_CREATE, perm)
    if err != nil {
        return nil, err
    }

    // Contruct data structure that implements DbFile interface
    res := &tDbFile{
        file:        file,
        access_mode: access_mode,
        perm:        perm,
    }

    return res, nil
}

package bolt

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Ensure that a database can be opened without error.
func TestDBOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Open(path, 0666)
		assert.NoError(t, err)
		assert.Equal(t, db.Path(), path)
	})
}

// Ensure that the database returns an error if already open.
func TestDBReopen(t *testing.T) {
	withDB(func(db *DB, path string) {
		db.Open(path, 0666)
		err := db.Open(path, 0666)
		assert.Equal(t, err, DatabaseAlreadyOpenedError)
	})
}

// Ensure that the database returns an error if the file handle cannot be open.
func TestDBOpenFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return((*mockfile)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that the database returns an error if the meta file handle cannot be open.
func TestDBOpenMetaFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(&mockfile{}, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return((*mockfile)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that the database limits the upper bound of the page size.
func TestDBLimitPageSize(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.init(0x8000)
		p1.init(0x8000)
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x10000, syscall.PROT_READ, syscall.MAP_SHARED).Return(b, nil)
		db.Open(path, 0666)
		assert.Equal(t, db.pageSize, maxPageSize)
	})
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDBMetaInitWriteError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, io.ErrShortWrite)
		err := db.Open(path, 0666)
		assert.Equal(t, err, io.ErrShortWrite)
	})
}

// Ensure that a database that is too small returns an error.
func TestDBFileTooSmall(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x1000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x1000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		err := db.Open(path, 0666)
		assert.Equal(t, err, &Error{"file size too small", nil})
	})
}

// Ensure that stat errors during mmap get returned.
func TestDBMmapStatError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x1000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return((*mockfileinfo)(nil), exp)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that mmap errors get returned.
func TestDBMmapError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := errors.New("")
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x1000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x2000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x2000, syscall.PROT_READ, syscall.MAP_SHARED).Return(([]byte)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.init(0x8000)
		p1.init(0x8000)
		m, _ := p0.meta()
		m.magic = 0
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x10000, syscall.PROT_READ, syscall.MAP_SHARED).Return(b, nil)
		err := db.Open(path, 0666)
		assert.Equal(t, err, &Error{"meta0 error", InvalidError})
	})
}

// Ensure that corrupt meta1 page errors get returned.
func TestDBCorruptMeta1(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.init(0x8000)
		p1.init(0x8000)
		m, _ := p1.meta()
		m.version = 100
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x10000, syscall.PROT_READ, syscall.MAP_SHARED).Return(b, nil)
		err := db.Open(path, 0666)
		assert.Equal(t, err, &Error{"meta1 error", VersionMismatchError})
	})
}

// withDB executes a function with a database reference.
func withDB(fn func(*DB, string)) {
	f, _ := ioutil.TempFile("", "bolt-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	db := NewDB()
	fn(db, path)
}

// withMockDB executes a function with a database reference and a mock filesystem.
func withMockDB(fn func(*DB, *mockos, *mocksyscall, string)) {
	os, syscall := &mockos{}, &mocksyscall{}
	db := NewDB()
	db.os = os
	db.syscall = syscall
	fn(db, os, syscall, "/mock/db")
}

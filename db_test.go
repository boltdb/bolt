package bolt

import (
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

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDBMetaInitWriteError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		// Mock the file system.
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("Stat").Return(&mockfileinfo{"", 0, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, io.ErrShortWrite)

		// Open the database.
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
		file.On("Stat").Return(&mockfileinfo{"", 0, 0666, time.Now(), false, nil}, nil)
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
		assert.Equal(t, err, &Error{"stat error", exp})
	})
}

// Ensure that mmap errors get returned.
/*
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
*/

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		// Create a file with bad magic.
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.init(0x8000)
		p1.init(0x8000)
		m, _ := p0.meta()
		m.magic = 0

		// Mock file access.
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x10000, syscall.PROT_READ, syscall.MAP_SHARED).Return(b, nil)

		// Open the database.
		err := db.Open(path, 0666)
		assert.Equal(t, err, &Error{"meta bootstrap error", InvalidMetaPageError})
	})
}


//--------------------------------------
// Transaction()
//--------------------------------------

// Ensure that a database cannot open a transaction when it's not open.
func TestDBTransactionDatabaseNotOpenError(t *testing.T) {
	withDB(func(db *DB, path string) {
		txn, err := db.Transaction()
		assert.Nil(t, txn)
		assert.Equal(t, err, DatabaseNotOpenError)
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

// withOpenDB executes a function with an already opened database.
func withOpenDB(fn func(*DB, string)) {
	withDB(func(db *DB, path string) {
		if err := db.Open(path, 0666); err != nil {
			panic("cannot open db: " + err.Error())
		}
		defer db.Close()
		fn(db, path)
	})
}

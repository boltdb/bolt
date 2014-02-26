package bolt

import (
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Ensure that a database can be opened without error.
func TestOpen(t *testing.T) {
	f, _ := ioutil.TempFile("", "bolt-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	db, err := Open(path, 0666)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	db.Close()
}

// Ensure that opening a database with a bad path returns an error.
func TestOpenBadPath(t *testing.T) {
	db, err := Open("/../bad-path", 0666)
	assert.Error(t, err)
	assert.Nil(t, db)
}

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
		assert.Equal(t, err, ErrDatabaseOpen)
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

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		var m meta
		m.magic = magic
		m.version = version
		m.pageSize = 0x8000

		// Create a file with bad magic.
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.meta().magic = 0
		p0.meta().version = version
		p1.meta().magic = magic
		p1.meta().version = version

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
		assert.Equal(t, err, &Error{"meta error", ErrInvalid})
	})
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDBTransactionErrDatabaseNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		txn, err := db.Transaction()
		assert.Nil(t, txn)
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure that a delete on a missing bucket returns an error.
func TestDBDeleteFromMissingBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Delete("widgets", []byte("foo"))
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure a database can provide a transactional block.
func TestDBTransactionBlock(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Do(func(txn *RWTransaction) error {
			txn.CreateBucket("widgets")
			b := txn.Bucket("widgets")
			b.Put([]byte("foo"), []byte("bar"))
			b.Put([]byte("baz"), []byte("bat"))
			b.Delete([]byte("foo"))
			return nil
		})
		assert.NoError(t, err)
		value, _ := db.Get("widgets", []byte("foo"))
		assert.Nil(t, value)
		value, _ = db.Get("widgets", []byte("baz"))
		assert.Equal(t, value, []byte("bat"))
	})
}

// Ensure a closed database returns an error while running a transaction block
func TestDBTransactionBlockWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Do(func(txn *RWTransaction) error {
			txn.CreateBucket("widgets")
			return nil
		})
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure a database returns an error when trying to attempt a for each on a missing bucket.
func TestDBForEachBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.ForEach("widgets", func(k, v []byte) error { return nil })
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure a closed database returns an error when executing a for each.
func TestDBForEachWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.ForEach("widgets", func(k, v []byte) error { return nil })
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure a closed database returns an error when finding a bucket.
func TestDBBucketWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		b, err := db.Bucket("widgets")
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, b)
	})
}

// Ensure a closed database returns an error when finding all buckets.
func TestDBBucketsWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		b, err := db.Buckets()
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, b)
	})
}

// Ensure a closed database returns an error when getting a key.
func TestDBGetWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		value, err := db.Get("widgets", []byte("foo"))
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, value)
	})
}

// Ensure that an error is returned when inserting into a bucket that doesn't exist.
func TestDBPutBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Put("widgets", []byte("foo"), []byte("bar"))
		assert.Equal(t, err, ErrBucketNotFound)
	})
}

// Ensure that the database can be copied to a file path.
func TestDBCopyFile(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		db.Put("widgets", []byte("baz"), []byte("bat"))
		assert.NoError(t, os.RemoveAll("/tmp/bolt.copyfile.db"))
		assert.NoError(t, db.CopyFile("/tmp/bolt.copyfile.db", 0666))

		var db2 DB
		assert.NoError(t, db2.Open("/tmp/bolt.copyfile.db", 0666))
		defer db2.Close()

		value, _ := db2.Get("widgets", []byte("foo"))
		assert.Equal(t, value, []byte("bar"))
		value, _ = db2.Get("widgets", []byte("baz"))
		assert.Equal(t, value, []byte("bat"))
	})
}

// Ensure the database can return stats about itself.
func TestDBStat(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Do(func(txn *RWTransaction) error {
			txn.CreateBucket("widgets")
			b := txn.Bucket("widgets")
			for i := 0; i < 10000; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			return nil
		})

		// Delete some keys.
		db.Delete("widgets", []byte("10"))
		db.Delete("widgets", []byte("1000"))

		// Open some readers.
		t0, _ := db.Transaction()
		t1, _ := db.Transaction()
		t2, _ := db.Transaction()
		t2.Close()

		// Obtain stats.
		stat, err := db.Stat()
		assert.NoError(t, err)
		assert.Equal(t, stat.PageCount, 128)
		assert.Equal(t, stat.FreePageCount, 2)
		assert.Equal(t, stat.PageSize, 4096)
		assert.Equal(t, stat.MmapSize, 4194304)
		assert.Equal(t, stat.TransactionCount, 2)

		// Close readers.
		t0.Close()
		t1.Close()
	})
}

// Ensure the getting stats on a closed database returns an error.
func TestDBStatWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		stat, err := db.Stat()
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, stat)
	})
}

// Ensure that an error is returned when a database write fails.
func TestDBWriteFail(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that the mmap grows appropriately.
func TestDBMmapSize(t *testing.T) {
	db := &DB{pageSize: 4096}
	assert.Equal(t, db.mmapSize(0), minMmapSize)
	assert.Equal(t, db.mmapSize(16384), minMmapSize)
	assert.Equal(t, db.mmapSize(minMmapSize-1), minMmapSize)
	assert.Equal(t, db.mmapSize(minMmapSize), minMmapSize*2)
	assert.Equal(t, db.mmapSize(10000000), 20000768)
	assert.Equal(t, db.mmapSize((1<<30)-1), 2147483648)
	assert.Equal(t, db.mmapSize(1<<30), 1<<31)
}

// withDB executes a function with a database reference.
func withDB(fn func(*DB, string)) {
	f, _ := ioutil.TempFile("", "bolt-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	var db DB
	fn(&db, path)
}

// withMockDB executes a function with a database reference and a mock filesystem.
func withMockDB(fn func(*DB, *mockos, *mocksyscall, string)) {
	os, syscall := &mockos{}, &mocksyscall{}
	var db DB
	db.os = os
	db.syscall = syscall
	fn(&db, os, syscall, "/mock/db")
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

func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

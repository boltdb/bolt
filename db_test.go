package bolt

import (
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
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
	withDB(func(db *DB, path string) {
		err := db.Open(path+"/youre-not-my-real-parent", 0666)
		if err, _ := err.(*os.PathError); assert.Error(t, err) {
			assert.Equal(t, path+"/youre-not-my-real-parent", err.Path)
			assert.Equal(t, "open", err.Op)
		}
	})
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDBMetaInitWriteError(t *testing.T) {
	withDB(func(db *DB, path string) {
		// Mock the file system.
		db.ops.metaWriteAt = func(p []byte, offset int64) (n int, err error) { return 0, io.ErrShortWrite }

		// Open the database.
		err := db.Open(path, 0666)
		assert.Equal(t, err, io.ErrShortWrite)
	})
}

// Ensure that a database that is too small returns an error.
func TestDBFileTooSmall(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Close()

		// corrupt the database
		assert.NoError(t, os.Truncate(path, int64(os.Getpagesize())))

		err := db.Open(path, 0666)
		assert.Equal(t, errors.New("file size too small"), err)
	})
}

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withDB(func(db *DB, path string) {
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
		err := ioutil.WriteFile(path, b, 0666)
		assert.NoError(t, err)

		// Open the database.
		err = db.Open(path, 0666)
		assert.Equal(t, err, errors.New("meta error: invalid database"))
	})
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDBTxErrDatabaseNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		tx, err := db.Begin(false)
		assert.Nil(t, tx)
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure that a read-write transaction can be retrieved.
func TestDBBeginRW(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, err := db.Begin(true)
		assert.NotNil(t, tx)
		assert.NoError(t, err)
		assert.Equal(t, tx.DB(), db)
		assert.Equal(t, tx.Writable(), true)
	})
}

// Ensure that opening a transaction while the DB is closed returns an error.
func TestDBRWTxOpenWithClosedDB(t *testing.T) {
	withDB(func(db *DB, path string) {
		tx, err := db.Begin(true)
		assert.Equal(t, err, ErrDatabaseNotOpen)
		assert.Nil(t, tx)
	})
}

// Ensure a database can provide a transactional block.
func TestDBTxBlock(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			b.Put([]byte("foo"), []byte("bar"))
			b.Put([]byte("baz"), []byte("bat"))
			b.Delete([]byte("foo"))
			return nil
		})
		assert.NoError(t, err)
		err = db.View(func(tx *Tx) error {
			assert.Nil(t, tx.Bucket("widgets").Get([]byte("foo")))
			assert.Equal(t, []byte("bat"), tx.Bucket("widgets").Get([]byte("baz")))
			return nil
		})
		assert.NoError(t, err)
	})
}

// Ensure a closed database returns an error while running a transaction block
func TestDBTxBlockWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			return nil
		})
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure a panic occurs while trying to commit a managed transaction.
func TestDBTxBlockWithManualCommitAndRollback(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			assert.Panics(t, func() { tx.Commit() })
			assert.Panics(t, func() { tx.Rollback() })
			return nil
		})
		db.View(func(tx *Tx) error {
			assert.Panics(t, func() { tx.Commit() })
			assert.Panics(t, func() { tx.Rollback() })
			return nil
		})
	})
}

// Ensure that the database can be copied to a file path.
func TestDBCopyFile(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			tx.Bucket("widgets").Put([]byte("foo"), []byte("bar"))
			tx.Bucket("widgets").Put([]byte("baz"), []byte("bat"))
			return nil
		})
		assert.NoError(t, os.RemoveAll("/tmp/bolt.copyfile.db"))
		assert.NoError(t, db.CopyFile("/tmp/bolt.copyfile.db", 0666))

		var db2 DB
		assert.NoError(t, db2.Open("/tmp/bolt.copyfile.db", 0666))
		defer db2.Close()

		db2.View(func(tx *Tx) error {
			assert.Equal(t, []byte("bar"), tx.Bucket("widgets").Get([]byte("foo")))
			assert.Equal(t, []byte("bat"), tx.Bucket("widgets").Get([]byte("baz")))
			return nil
		})
	})
}

// Ensure the database can return stats about itself.
func TestDBStat(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket("widgets")
			b := tx.Bucket("widgets")
			for i := 0; i < 10000; i++ {
				b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
			return nil
		})

		// Delete some keys.
		db.Update(func(tx *Tx) error {
			return tx.Bucket("widgets").Delete([]byte("10"))
		})
		db.Update(func(tx *Tx) error {
			return tx.Bucket("widgets").Delete([]byte("1000"))
		})

		// Open some readers.
		t0, _ := db.Begin(false)
		t1, _ := db.Begin(false)
		t2, _ := db.Begin(false)
		t2.Rollback()

		// Obtain stats.
		stat, err := db.Stat()
		assert.NoError(t, err)
		assert.Equal(t, stat.PageCount, 128)
		assert.Equal(t, stat.FreePageCount, 2)
		assert.Equal(t, stat.PageSize, 4096)
		assert.Equal(t, stat.MmapSize, 4194304)
		assert.Equal(t, stat.TxCount, 2)

		// Close readers.
		t0.Rollback()
		t1.Rollback()
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

// Ensure that a database can return a string representation of itself.
func TestDBString(t *testing.T) {
	db := &DB{path: "/tmp/foo"}
	assert.Equal(t, db.String(), `DB<"/tmp/foo">`)
	assert.Equal(t, db.GoString(), `bolt.DB{path:"/tmp/foo"}`)
}

// Benchmark the performance of single put transactions in random order.
func BenchmarkDBPutSequential(b *testing.B) {
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})
		for i := 0; i < b.N; i++ {
			db.Update(func(tx *Tx) error {
				return tx.Bucket("widgets").Put([]byte(strconv.Itoa(i)), value)
			})
		}
	})
}

// Benchmark the performance of single put transactions in random order.
func BenchmarkDBPutRandom(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})
		for i := 0; i < b.N; i++ {
			db.Update(func(tx *Tx) error {
				return tx.Bucket("widgets").Put([]byte(strconv.Itoa(indexes[i])), value)
			})
		}
	})
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

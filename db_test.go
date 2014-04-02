package bolt

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var statsFlag = flag.Bool("stats", false, "show performance stats")

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
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		assert.NotNil(t, db)
		assert.NoError(t, err)
		assert.Equal(t, db.Path(), path)
		assert.NoError(t, db.Close())
	})
}

// Ensure that a re-opened database is consistent.
func TestOpenCheck(t *testing.T) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		assert.NoError(t, err)
		assert.NoError(t, db.Check())
		db.Close()

		db, err = Open(path, 0666)
		assert.NoError(t, err)
		assert.NoError(t, db.Check())
		db.Close()
	})
}

// Ensure that the database returns an error if the file handle cannot be open.
func TestDBOpenFileError(t *testing.T) {
	withTempPath(func(path string) {
		_, err := Open(path+"/youre-not-my-real-parent", 0666)
		if err, _ := err.(*os.PathError); assert.Error(t, err) {
			assert.Equal(t, path+"/youre-not-my-real-parent", err.Path)
			assert.Equal(t, "open", err.Op)
		}
	})
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDBMetaInitWriteError(t *testing.T) {
	t.Skip("pending")
}

// Ensure that a database that is too small returns an error.
func TestDBFileTooSmall(t *testing.T) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		assert.NoError(t, err)
		db.Close()

		// corrupt the database
		assert.NoError(t, os.Truncate(path, int64(os.Getpagesize())))

		db, err = Open(path, 0666)
		assert.Equal(t, errors.New("file size too small"), err)
	})
}

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withTempPath(func(path string) {
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
		_, err = Open(path, 0666)
		assert.Equal(t, err, errors.New("meta error: invalid database"))
	})
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDBTxErrDatabaseNotOpen(t *testing.T) {
	var db DB
	tx, err := db.Begin(false)
	assert.Nil(t, tx)
	assert.Equal(t, err, ErrDatabaseNotOpen)
}

// Ensure that a read-write transaction can be retrieved.
func TestDBBeginRW(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, err := db.Begin(true)
		assert.NotNil(t, tx)
		assert.NoError(t, err)
		assert.Equal(t, tx.DB(), db)
		assert.Equal(t, tx.Writable(), true)
		assert.NoError(t, tx.Commit())
	})
}

// Ensure that opening a transaction while the DB is closed returns an error.
func TestDBRWTxOpenWithClosedDB(t *testing.T) {
	var db DB
	tx, err := db.Begin(true)
	assert.Equal(t, err, ErrDatabaseNotOpen)
	assert.Nil(t, tx)
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
	var db DB
	err := db.Update(func(tx *Tx) error {
		tx.CreateBucket("widgets")
		return nil
	})
	assert.Equal(t, err, ErrDatabaseNotOpen)
}

// Ensure a panic occurs while trying to commit a managed transaction.
func TestDBTxBlockWithManualCommitAndRollback(t *testing.T) {
	var db DB
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

		db2, err := Open("/tmp/bolt.copyfile.db", 0666)
		assert.NoError(t, err)
		defer db2.Close()

		db2.View(func(tx *Tx) error {
			assert.Equal(t, []byte("bar"), tx.Bucket("widgets").Get([]byte("foo")))
			assert.Equal(t, []byte("bat"), tx.Bucket("widgets").Get([]byte("baz")))
			return nil
		})
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

// Ensure that database pages are in expected order and type.
func TestDBConsistency(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			return tx.CreateBucket("widgets")
		})

		for i := 0; i < 10; i++ {
			db.Update(func(tx *Tx) error {
				assert.NoError(t, tx.Bucket("widgets").Put([]byte("foo"), []byte("bar")))
				return nil
			})
		}
		db.Update(func(tx *Tx) error {
			if p, _ := tx.Page(0); assert.NotNil(t, p) {
				assert.Equal(t, "meta", p.Type)
			}
			if p, _ := tx.Page(1); assert.NotNil(t, p) {
				assert.Equal(t, "meta", p.Type)
			}
			if p, _ := tx.Page(2); assert.NotNil(t, p) {
				assert.Equal(t, "free", p.Type)
			}
			if p, _ := tx.Page(3); assert.NotNil(t, p) {
				assert.Equal(t, "free", p.Type)
			}
			if p, _ := tx.Page(4); assert.NotNil(t, p) {
				assert.Equal(t, "freelist", p.Type)
			}
			if p, _ := tx.Page(5); assert.NotNil(t, p) {
				assert.Equal(t, "buckets", p.Type)
			}
			if p, _ := tx.Page(6); assert.NotNil(t, p) {
				assert.Equal(t, "leaf", p.Type)
			}
			if p, _ := tx.Page(7); assert.NotNil(t, p) {
				assert.Equal(t, "free", p.Type)
			}
			p, _ := tx.Page(8)
			assert.Nil(t, p)
			return nil
		})
	})
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

// withTempPath executes a function with a database reference.
func withTempPath(fn func(string)) {
	f, _ := ioutil.TempFile("", "bolt-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	fn(path)
}

// withOpenDB executes a function with an already opened database.
func withOpenDB(fn func(*DB, string)) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		if err != nil {
			panic("cannot open db: " + err.Error())
		}
		defer db.Close()
		fn(db, path)

		// Log statistics.
		if *statsFlag {
			logStats(db)
		}

		// Check database consistency after every test.
		mustCheck(db)
	})
}

// mustCheck runs a consistency check on the database and panics if any errors are found.
func mustCheck(db *DB) {
	if err := db.Check(); err != nil {
		// Copy db off first.
		db.CopyFile("/tmp/check.db", 0600)

		if errors, ok := err.(ErrorList); ok {
			for _, err := range errors {
				warn(err)
			}
		}
		warn(err)
		panic("check failure: see /tmp/check.db")
	}
}

func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

// writes the current database stats to the testing log.
func logStats(db *DB) {
	var stats = db.Stats()
	fmt.Printf("[db] %-20s %-20s %-20s\n",
		fmt.Sprintf("pg(%d/%d)", stats.TxStats.PageCount, stats.TxStats.PageAlloc),
		fmt.Sprintf("cur(%d)", stats.TxStats.CursorCount),
		fmt.Sprintf("node(%d/%d)", stats.TxStats.NodeCount, stats.TxStats.NodeDeref),
	)
	fmt.Printf("     %-20s %-20s %-20s\n",
		fmt.Sprintf("rebal(%d/%v)", stats.TxStats.Rebalance, truncDuration(stats.TxStats.RebalanceTime)),
		fmt.Sprintf("spill(%d/%v)", stats.TxStats.Spill, truncDuration(stats.TxStats.SpillTime)),
		fmt.Sprintf("w(%d/%v)", stats.TxStats.Write, truncDuration(stats.TxStats.WriteTime)),
	)
}

func truncDuration(d time.Duration) string {
	return regexp.MustCompile(`^(\d+)(\.\d+)`).ReplaceAllString(d.String(), "$1")
}

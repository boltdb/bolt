package bolt

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
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
func TestOpen_BadPath(t *testing.T) {
	db, err := Open("/../bad-path", 0666)
	assert.Error(t, err)
	assert.Nil(t, db)
}

// Ensure that a database can be opened without error.
func TestDB_Open(t *testing.T) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		assert.NotNil(t, db)
		assert.NoError(t, err)
		assert.Equal(t, db.Path(), path)
		assert.NoError(t, db.Close())
	})
}

// Ensure that a re-opened database is consistent.
func TestOpen_Check(t *testing.T) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		assert.NoError(t, err)
		assert.NoError(t, db.View(func(tx *Tx) error { return <-tx.Check() }))
		db.Close()

		db, err = Open(path, 0666)
		assert.NoError(t, err)
		assert.NoError(t, db.View(func(tx *Tx) error { return <-tx.Check() }))
		db.Close()
	})
}

// Ensure that the database returns an error if the file handle cannot be open.
func TestDB_Open_FileError(t *testing.T) {
	withTempPath(func(path string) {
		_, err := Open(path+"/youre-not-my-real-parent", 0666)
		if err, _ := err.(*os.PathError); assert.Error(t, err) {
			assert.Equal(t, path+"/youre-not-my-real-parent", err.Path)
			assert.Equal(t, "open", err.Op)
		}
	})
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDB_Open_MetaInitWriteError(t *testing.T) {
	t.Skip("pending")
}

// Ensure that a database that is too small returns an error.
func TestDB_Open_FileTooSmall(t *testing.T) {
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
func TestDB_Open_CorruptMeta0(t *testing.T) {
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
		assert.Equal(t, err, errors.New("meta0 error: invalid database"))
	})
}

// Ensure that a corrupt meta page checksum causes the open to fail.
func TestDB_Open_MetaChecksumError(t *testing.T) {
	for i := 0; i < 2; i++ {
		withTempPath(func(path string) {
			db, err := Open(path, 0600)
			pageSize := db.pageSize
			db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("widgets"))
				return err
			})
			db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("woojits"))
				return err
			})
			db.Close()

			// Change a single byte in the meta page.
			f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
			f.WriteAt([]byte{1}, int64((i*pageSize)+(pageHeaderSize+12)))
			f.Sync()
			f.Close()

			// Reopen the database.
			_, err = Open(path, 0600)
			if assert.Error(t, err) {
				if i == 0 {
					assert.Equal(t, "meta0 error: checksum error", err.Error())
				} else {
					assert.Equal(t, "meta1 error: checksum error", err.Error())
				}
			}
		})
	}
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDB_Begin_DatabaseNotOpen(t *testing.T) {
	var db DB
	tx, err := db.Begin(false)
	assert.Nil(t, tx)
	assert.Equal(t, err, ErrDatabaseNotOpen)
}

// Ensure that a read-write transaction can be retrieved.
func TestDB_BeginRW(t *testing.T) {
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
func TestDB_BeginRW_Closed(t *testing.T) {
	var db DB
	tx, err := db.Begin(true)
	assert.Equal(t, err, ErrDatabaseNotOpen)
	assert.Nil(t, tx)
}

// Ensure a database can provide a transactional block.
func TestDB_Update(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			b.Put([]byte("foo"), []byte("bar"))
			b.Put([]byte("baz"), []byte("bat"))
			b.Delete([]byte("foo"))
			return nil
		})
		assert.NoError(t, err)
		err = db.View(func(tx *Tx) error {
			assert.Nil(t, tx.Bucket([]byte("widgets")).Get([]byte("foo")))
			assert.Equal(t, []byte("bat"), tx.Bucket([]byte("widgets")).Get([]byte("baz")))
			return nil
		})
		assert.NoError(t, err)
	})
}

// Ensure a closed database returns an error while running a transaction block
func TestDB_Update_Closed(t *testing.T) {
	var db DB
	err := db.Update(func(tx *Tx) error {
		tx.CreateBucket([]byte("widgets"))
		return nil
	})
	assert.Equal(t, err, ErrDatabaseNotOpen)
}

// Ensure a panic occurs while trying to commit a managed transaction.
func TestDB_Update_ManualCommitAndRollback(t *testing.T) {
	var db DB
	db.Update(func(tx *Tx) error {
		tx.CreateBucket([]byte("widgets"))
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

// Ensure a database can return an error through a read-only transactional block.
func TestDB_View_Error(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.View(func(tx *Tx) error {
			return errors.New("xxx")
		})
		assert.Equal(t, errors.New("xxx"), err)
	})
}

// Ensure that an error is returned when a database write fails.
func TestDB_Commit_WriteFail(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that DB stats can be returned.
func TestDB_Stats(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})
		stats := db.Stats()
		assert.Equal(t, 2, stats.TxStats.PageCount)
	})
}

// Ensure that the mmap grows appropriately.
func TestDB_mmapSize(t *testing.T) {
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
func TestDB_Consistency(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})

		for i := 0; i < 10; i++ {
			db.Update(func(tx *Tx) error {
				assert.NoError(t, tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")))
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
				assert.Equal(t, "leaf", p.Type) // root leaf
			}
			if p, _ := tx.Page(5); assert.NotNil(t, p) {
				assert.Equal(t, "freelist", p.Type)
			}
			p, _ := tx.Page(6)
			assert.Nil(t, p)
			return nil
		})
	})
}

// Ensure that a database can return a string representation of itself.
func TestDB_String(t *testing.T) {
	db := &DB{path: "/foo/bar"}
	assert.Equal(t, db.String(), `DB<"/foo/bar">`)
	assert.Equal(t, db.GoString(), `bolt.DB{path:"/foo/bar"}`)
}

// Ensure that DB stats can be substracted from one another.
func TestDBStats_Sub(t *testing.T) {
	var a, b Stats
	a.TxStats.PageCount = 3
	b.TxStats.PageCount = 10
	diff := b.Sub(&a)
	assert.Equal(t, 7, diff.TxStats.PageCount)
}

// Ensure that meta with bad magic is invalid.
func TestMeta_validate_magic(t *testing.T) {
	m := &meta{magic: 0x01234567}
	assert.Equal(t, m.validate(), ErrInvalid)
}

// Ensure that meta with a bad version is invalid.
func TestMeta_validate_version(t *testing.T) {
	m := &meta{magic: magic, version: 200}
	assert.Equal(t, m.validate(), ErrVersionMismatch)
}

// Ensure that a DB in strict mode will fail when corrupted.
func TestDB_StrictMode(t *testing.T) {
	var msg string
	func() {
		defer func() {
			msg = fmt.Sprintf("%s", recover())
		}()

		withOpenDB(func(db *DB, path string) {
			db.StrictMode = true
			db.Update(func(tx *Tx) error {
				tx.CreateBucket([]byte("foo"))

				// Corrupt the DB by extending the high water mark.
				tx.meta.pgid++

				return nil
			})
		})
	}()

	assert.Equal(t, "check fail: page 4: unreachable unfreed", msg)
}

// Ensure that a double freeing a page will result in a panic.
func TestDB_DoubleFree(t *testing.T) {
	var msg string
	func() {
		defer func() {
			msg = fmt.Sprintf("%s", recover())
		}()
		withOpenDB(func(db *DB, path string) {
			db.Update(func(tx *Tx) error {
				tx.CreateBucket([]byte("foo"))

				// Corrupt the DB by adding a page to the freelist.
				db.freelist.free(0, tx.page(3))

				return nil
			})
		})
	}()

	assert.Equal(t, "tx 2: page 3 already freed in tx 0", msg)
}

func ExampleDB_Update() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Execute several commands within a write transaction.
	err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	})

	// If our transactional block didn't return an error then our data is saved.
	if err == nil {
		db.View(func(tx *Tx) error {
			value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
			fmt.Printf("The value of 'foo' is: %s\n", value)
			return nil
		})
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleDB_View() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Insert data into a bucket.
	db.Update(func(tx *Tx) error {
		tx.CreateBucket([]byte("people"))
		b := tx.Bucket([]byte("people"))
		b.Put([]byte("john"), []byte("doe"))
		b.Put([]byte("susy"), []byte("que"))
		return nil
	})

	// Access data from within a read-only transactional block.
	db.View(func(tx *Tx) error {
		v := tx.Bucket([]byte("people")).Get([]byte("john"))
		fmt.Printf("John's last name is %s.\n", v)
		return nil
	})

	// Output:
	// John's last name is doe.
}

func ExampleDB_Begin_ReadOnly() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Create a bucket.
	db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})

	// Create several keys in a transaction.
	tx, _ := db.Begin(true)
	b := tx.Bucket([]byte("widgets"))
	b.Put([]byte("john"), []byte("blue"))
	b.Put([]byte("abby"), []byte("red"))
	b.Put([]byte("zephyr"), []byte("purple"))
	tx.Commit()

	// Iterate over the values in sorted key order.
	tx, _ = db.Begin(false)
	c := tx.Bucket([]byte("widgets")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", k, v)
	}
	tx.Rollback()

	// Output:
	// abby likes red
	// john likes blue
	// zephyr likes purple
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// withTempPath executes a function with a database reference.
func withTempPath(fn func(string)) {
	path := tempfile()
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
	err := db.Update(func(tx *Tx) error {
		return <-tx.Check()
	})
	if err != nil {
		// Copy db off first.
		var path = tempfile()
		db.View(func(tx *Tx) error { return tx.CopyFile(path, 0600) })
		panic("check failure: " + err.Error() + ": " + path)
	}
}

// mustContainKeys checks that a bucket contains a given set of keys.
func mustContainKeys(b *Bucket, m map[string]string) {
	found := make(map[string]string)
	b.ForEach(func(k, _ []byte) error {
		found[string(k)] = ""
		return nil
	})

	// Check for keys found in bucket that shouldn't be there.
	var keys []string
	for k, _ := range found {
		if _, ok := m[string(k)]; !ok {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		panic(fmt.Sprintf("keys found(%d): %s", len(keys), strings.Join(keys, ",")))
	}

	// Check for keys not found in bucket that should be there.
	for k, _ := range m {
		if _, ok := found[string(k)]; !ok {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		panic(fmt.Sprintf("keys not found(%d): %s", len(keys), strings.Join(keys, ",")))
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

// copyAndFailNow copies a database to a new location and then fails then test.
func copyAndFailNow(t *testing.T, db *DB) {
	path := tempfile()
	db.View(func(tx *Tx) error { return tx.CopyFile(path, 0600) })
	fmt.Println("db copied to: ", path)
	t.FailNow()
}

package c

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"testing/quick"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
)

// Test when cursor hits the end
// Implement seek; binary search within the page (branch page and element page)

// Ensure that a cursor can get the first element of a bucket.
func TestCursorFirst(t *testing.T) {
	withOpenDB(func(db *bolt.DB, path string) {

		// Bulk insert all values.
		tx, _ := db.Begin(true)
		b, _ := tx.CreateBucket(toBytes("widgets"))
		assert.NoError(t, b.Put(toBytes("foo"), toBytes("barz")))
		assert.NoError(t, tx.Commit())

		// Get first and check consistency
		tx, _ = db.Begin(false)
		c := NewCursor(tx.Bucket(toBytes("widgets")))
		key, value := first(c)
		assert.Equal(t, key, toBytes("foo"))
		assert.Equal(t, value, toBytes("barz"))

		tx.Rollback()
	})
}

// Ensure that a cursor can iterate over all elements in a bucket.
func TestIterate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	f := func(items testdata) bool {
		withOpenDB(func(db *bolt.DB, path string) {
			// Bulk insert all values.
			tx, _ := db.Begin(true)
			b, _ := tx.CreateBucket(toBytes("widgets"))
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, tx.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			tx, _ = db.Begin(false)
			c := NewCursor(tx.Bucket(toBytes("widgets")))
			for key, value := first(c); key != nil && index < len(items); key, value = next(c) {
				assert.Equal(t, key, items[index].Key)
				assert.Equal(t, value, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			assert.Equal(t, len(items), index)
			tx.Rollback()
		})
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

// toBytes converts a string to an array of bytes.
func toBytes(s string) []byte {
	return []byte(s)
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
func withOpenDB(fn func(*bolt.DB, string)) {
	withTempPath(func(path string) {
		db, err := bolt.Open(path, 0666)
		if err != nil {
			panic("cannot open db: " + err.Error())
		}
		defer db.Close()
		fn(db, path)

		// Log statistics.
		// if *statsFlag {
		// 	logStats(db)
		// }

		// Check database consistency after every test.
		mustCheck(db)
	})
}

// mustCheck runs a consistency check on the database and panics if any errors are found.
func mustCheck(db *bolt.DB) {
	if err := db.Check(); err != nil {
		// Copy db off first.
		db.CopyFile("/tmp/check.db", 0600)

		if errors, ok := err.(bolt.ErrorList); ok {
			for _, err := range errors {
				warn(err)
			}
		}
		warn(err)
		panic("check failure: see /tmp/check.db")
	}
}

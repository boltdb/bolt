package c_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/bolt/c"
	"github.com/stretchr/testify/assert"
)

// Ensure that the C cursor can
func TestCursor_First(t *testing.T) {
	withDB(func(db *bolt.DB) {
		db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucket([]byte("widgets"))
			return b.Put([]byte("foo"), []byte("barz"))
		})
		db.View(func(tx *bolt.Tx) error {
			c := NewCursor(tx.Bucket([]byte("widgets")))
			key, value := c.First()
			assert.Equal(t, []byte("foo"), key)
			assert.Equal(t, []byte("barz"), value)
			return nil
		})
	})
}

// Ensure that a C cursor can iterate over a single root with a couple elements.
func TestCursor_Iterate_Leaf(t *testing.T) {
	withDB(func(db *bolt.DB) {
		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("baz"), []byte{})
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte{0})
			tx.Bucket([]byte("widgets")).Put([]byte("bar"), []byte{1})
			return nil
		})
		db.View(func(tx *bolt.Tx) error {
			c := NewCursor(tx.Bucket([]byte("widgets")))

			k, v := c.First()
			assert.Equal(t, string(k), "bar")
			assert.Equal(t, []byte{1}, v)

			k, v = c.Next()
			assert.Equal(t, string(k), "baz")
			assert.Equal(t, []byte{}, v)

			k, v = c.Next()
			assert.Equal(t, string(k), "foo")
			assert.Equal(t, []byte{0}, v)

			k, v = c.Next()
			assert.Equal(t, []byte{}, k)
			assert.Equal(t, []byte{}, v)

			k, v = c.Next()
			assert.Equal(t, []byte{}, k)
			assert.Equal(t, []byte{}, v)
			return nil
		})
	})
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-c-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// withDB executes a function with an already opened database.
func withDB(fn func(*bolt.DB)) {
	path := tempfile()
	db, err := bolt.Open(path, 0666)
	if err != nil {
		panic("cannot open db: " + err.Error())
	}
	defer os.Remove(path)
	defer db.Close()

	fn(db)

	// Check database consistency after every test.
	mustCheck(db)
}

// mustCheck runs a consistency check on the database and panics if any errors are found.
func mustCheck(db *bolt.DB) {
	if err := db.Check(); err != nil {
		// Copy db off first.
		var path = tempfile()
		db.CopyFile(path, 0600)

		if errors, ok := err.(bolt.ErrorList); ok {
			for _, err := range errors {
				fmt.Println(err)
			}
		}
		fmt.Println(err)
		panic("check failure: " + path)
	}
}

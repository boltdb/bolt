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

// Ensure that a C cursor can seek to the appropriate keys.
func TestCursor_Seek(t *testing.T) {
	withDB(func(db *bolt.DB) {
		db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NoError(t, err)
			assert.NoError(t, b.Put([]byte("foo"), []byte("0001")))
			assert.NoError(t, b.Put([]byte("bar"), []byte("0002")))
			assert.NoError(t, b.Put([]byte("baz"), []byte("0003")))
			_, err = b.CreateBucket([]byte("bkt"))
			assert.NoError(t, err)
			return nil
		})
		db.View(func(tx *bolt.Tx) error {
			c := NewCursor(tx.Bucket([]byte("widgets")))

			// Exact match should go to the key.
			k, v, flags := c.Seek([]byte("bar"))
			assert.Equal(t, "bar", string(k))
			assert.Equal(t, "0002", string(v))
			assert.Equal(t, 0, flags)

			// Inexact match should go to the next key.
			k, v, flags = c.Seek([]byte("bas"))
			assert.Equal(t, "baz", string(k))
			assert.Equal(t, "0003", string(v))
			assert.Equal(t, 0, flags)

			// Low key should go to the first key.
			k, v, flags = c.Seek([]byte(""))
			assert.Equal(t, "bar", string(k))
			assert.Equal(t, "0002", string(v))
			assert.Equal(t, 0, flags)

			// High key should return no key.
			k, v, flags = c.Seek([]byte("zzz"))
			assert.Equal(t, "", string(k))
			assert.Equal(t, "", string(v))
			assert.Equal(t, 0, flags)

			// Buckets should return their key but no value.
			k, v, flags = c.Seek([]byte("bkt"))
			assert.Equal(t, []byte("bkt"), k)
			assert.True(t, len(v) > 0)
			assert.Equal(t, 1, flags) // bucketLeafFlag

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

// Ensure that a C cursor can iterate over a branches and leafs.
func TestCursor_Iterate_Large(t *testing.T) {
	withDB(func(db *bolt.DB) {
		db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucket([]byte("widgets"))
			for i := 0; i < 1000; i++ {
				b.Put([]byte(fmt.Sprintf("%05d", i)), []byte(fmt.Sprintf("%020d", i)))
			}
			return nil
		})
		db.View(func(tx *bolt.Tx) error {
			var index int
			c := NewCursor(tx.Bucket([]byte("widgets")))
			for k, v := c.First(); len(k) > 0; k, v = c.Next() {
				assert.Equal(t, fmt.Sprintf("%05d", index), string(k))
				assert.Equal(t, fmt.Sprintf("%020d", index), string(v))
				index++
			}
			assert.Equal(t, 1000, index)
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

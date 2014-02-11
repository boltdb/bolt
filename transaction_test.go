package bolt

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that the database can retrieve a list of buckets.
func TestTransactionBuckets(t *testing.T) {
	t.Skip("pending") // TODO(benbjohnson)
}

// Ensure that a Transaction can retrieve a bucket.
func TestTransactionBucketMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		b, err := db.Bucket("widgets")
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			assert.Equal(t, "widgets", b.Name())
		}
	})
}

// Ensure that a Transaction retrieving a non-existent key returns nil.
func TestTransactionGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		value, err := db.Get("widgets", []byte("no_such_key"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

// Ensure that a Transaction cursor can iterate over an empty bucket without error.
func TestTransactionCursorEmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		txn, _ := db.Transaction()
		c := txn.Cursor("widgets")
		k, v := c.First()
		assert.Nil(t, k)
		assert.Nil(t, v)
		txn.Close()
	})
}

// Ensure that a Transaction returns a nil when a bucket doesn't exist.
func TestTransactionCursorMissingBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		txn, _ := db.Transaction()
		assert.Nil(t, txn.Cursor("woojits"))
		txn.Close()
	})
}

// Ensure that a Transaction cursor can iterate over a single root with a couple elements.
func TestTransactionCursorLeafRoot(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		txn, _ := db.Transaction()
		c := txn.Cursor("widgets")

		k, v := c.First()
		assert.Equal(t, string(k), "bar")
		assert.Equal(t, v, []byte{1})

		k, v = c.Next()
		assert.Equal(t, string(k), "baz")
		assert.Equal(t, v, []byte{})

		k, v = c.Next()
		assert.Equal(t, string(k), "foo")
		assert.Equal(t, v, []byte{0})

		k, v = c.Next()
		assert.Nil(t, k)
		assert.Nil(t, v)

		k, v = c.Next()
		assert.Nil(t, k)
		assert.Nil(t, v)

		txn.Close()
	})
}

// Ensure that a Transaction cursor can restart from the beginning.
func TestTransactionCursorRestart(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("bar"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{})

		txn, _ := db.Transaction()
		c := txn.Cursor("widgets")

		k, _ := c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		k, _ = c.First()
		assert.Equal(t, string(k), "bar")

		k, _ = c.Next()
		assert.Equal(t, string(k), "foo")

		txn.Close()
	})
}

// Ensure that a transaction can iterate over all elements in a bucket.
func TestTransactionCursorIterate(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTransaction()
			for _, item := range items {
				assert.NoError(t, rwtxn.Put("widgets", item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			txn, _ := db.Transaction()
			c := txn.Cursor("widgets")
			for k, v := c.First(); k != nil && index < len(items); k, v = c.Next() {
				assert.Equal(t, k, items[index].Key)
				assert.Equal(t, v, items[index].Value)
				index++
			}
			assert.Equal(t, len(items), index)
			txn.Close()
		})
		fmt.Fprint(os.Stderr, ".")
		return true
	}
	if err := quick.Check(f, qconfig()); err != nil {
		t.Error(err)
	}
	fmt.Fprint(os.Stderr, "\n")
}

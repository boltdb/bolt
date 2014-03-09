package bolt

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
)

// Ensure that the database can retrieve a list of buckets.
func TestTxBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("foo")
		db.CreateBucket("bar")
		db.CreateBucket("baz")
		buckets, err := db.Buckets()
		if assert.NoError(t, err) && assert.Equal(t, len(buckets), 3) {
			assert.Equal(t, buckets[0].Name(), "bar")
			assert.Equal(t, buckets[1].Name(), "baz")
			assert.Equal(t, buckets[2].Name(), "foo")
		}
	})
}

// Ensure that a Tx can retrieve a bucket.
func TestTxBucketMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		b, err := db.Bucket("widgets")
		assert.NoError(t, err)
		if assert.NotNil(t, b) {
			assert.Equal(t, "widgets", b.Name())
		}
	})
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTxGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("foo"), []byte("bar"))
		value, err := db.Get("widgets", []byte("no_such_key"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

// Ensure that a Tx cursor can iterate over an empty bucket without error.
func TestTxCursorEmptyBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		txn, _ := db.Tx()
		c := txn.Bucket("widgets").Cursor()
		k, v := c.First()
		assert.Nil(t, k)
		assert.Nil(t, v)
		txn.Close()
	})
}

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
func TestTxCursorLeafRoot(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		txn, _ := db.Tx()
		c := txn.Bucket("widgets").Cursor()

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

// Ensure that a Tx cursor can iterate in reverse over a single root with a couple elements.
func TestTxCursorLeafRootReverse(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("baz"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{0})
		db.Put("widgets", []byte("bar"), []byte{1})
		txn, _ := db.Tx()
		c := txn.Bucket("widgets").Cursor()

		k, v := c.Last()
		assert.Equal(t, string(k), "foo")
		assert.Equal(t, v, []byte{0})

		k, v = c.Prev()
		assert.Equal(t, string(k), "baz")
		assert.Equal(t, v, []byte{})

		k, v = c.Prev()
		assert.Equal(t, string(k), "bar")
		assert.Equal(t, v, []byte{1})

		k, v = c.Prev()
		assert.Nil(t, k)
		assert.Nil(t, v)

		k, v = c.Prev()
		assert.Nil(t, k)
		assert.Nil(t, v)

		txn.Close()
	})
}

// Ensure that a Tx cursor can restart from the beginning.
func TestTxCursorRestart(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.CreateBucket("widgets")
		db.Put("widgets", []byte("bar"), []byte{})
		db.Put("widgets", []byte("foo"), []byte{})

		txn, _ := db.Tx()
		c := txn.Bucket("widgets").Cursor()

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

// Ensure that a Tx can iterate over all elements in a bucket.
func TestTxCursorIterate(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTx()
			b := rwtxn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Sort test data.
			sort.Sort(items)

			// Iterate over all items and check consistency.
			var index = 0
			txn, _ := db.Tx()
			c := txn.Bucket("widgets").Cursor()
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

// Ensure that a transaction can iterate over all elements in a bucket in reverse.
func TestTxCursorIterateReverse(t *testing.T) {
	f := func(items testdata) bool {
		withOpenDB(func(db *DB, path string) {
			// Bulk insert all values.
			db.CreateBucket("widgets")
			rwtxn, _ := db.RWTx()
			b := rwtxn.Bucket("widgets")
			for _, item := range items {
				assert.NoError(t, b.Put(item.Key, item.Value))
			}
			assert.NoError(t, rwtxn.Commit())

			// Sort test data.
			sort.Sort(revtestdata(items))

			// Iterate over all items and check consistency.
			var index = 0
			txn, _ := db.Tx()
			c := txn.Bucket("widgets").Cursor()
			for k, v := c.Last(); k != nil && index < len(items); k, v = c.Prev() {
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

// Benchmark the performance iterating over a cursor.
func BenchmarkTxCursor(b *testing.B) {
	indexes := rand.Perm(b.N)
	value := []byte(strings.Repeat("0", 64))

	withOpenDB(func(db *DB, path string) {
		// Write data to bucket.
		db.CreateBucket("widgets")
		db.Do(func(txn *RWTx) error {
			bucket := txn.Bucket("widgets")
			for i := 0; i < b.N; i++ {
				bucket.Put([]byte(strconv.Itoa(indexes[i])), value)
			}
			return nil
		})
		b.ResetTimer()

		// Iterate over bucket using cursor.
		db.With(func(txn *Tx) error {
			count := 0
			c := txn.Bucket("widgets").Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				count++
			}
			if count != b.N {
				b.Fatalf("wrong count: %d; expected: %d", count, b.N)
			}
			return nil
		})
	})
}
